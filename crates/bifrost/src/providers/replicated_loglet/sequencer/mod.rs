// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod append;
mod node;

use std::sync::Arc;

use futures::channel::oneshot;
use tokio::sync::{mpsc, Semaphore};

use restate_core::{
    cancellation_watcher,
    network::{rpc_router::ResponseTracker, MessageRouterBuilder, Networking},
    task_center, Metadata, ShutdownError, TaskKind,
};
use restate_types::{
    logs::{LogletOffset, Record, SequenceNumber, TailState},
    net::log_server::Stored,
    replicated_loglet::{NodeSet, ReplicatedLogletId},
    GenerationalNodeId,
};

use super::replication::spread_selector::SpreadSelector;
use crate::loglet::{util::TailOffsetWatch, LogletCommit};
use append::Appender;
use node::RemoteLogServerManager;

#[derive(thiserror::Error, Debug)]
pub enum SequencerError {
    #[error("loglet offset exhausted")]
    LogletOffsetExhausted,
    #[error("batch exceeds possible length")]
    InvalidBatchLength,
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
}

/// A sharable part of the sequencer state. This is shared with node workers
#[derive(Debug)]
pub(crate) struct SequencerGlobalState {
    node_id: GenerationalNodeId,
    loglet_id: ReplicatedLogletId,
    committed_tail: TailOffsetWatch,
}

impl SequencerGlobalState {
    pub fn node_id(&self) -> &GenerationalNodeId {
        &self.node_id
    }

    pub fn loglet_id(&self) -> &ReplicatedLogletId {
        &self.loglet_id
    }

    pub fn committed_tail(&self) -> &TailOffsetWatch {
        &self.committed_tail
    }
}

/// internal commands sent over the [`SequencerHandler`] to sequencer main loop
struct Call<Q, A> {
    request: Q,
    sender: oneshot::Sender<A>,
}

impl<Q, A> Call<Q, A> {
    fn from_request(request: Q) -> (oneshot::Receiver<A>, Self) {
        let (sender, receiver) = oneshot::channel();
        (receiver, Self { request, sender })
    }
}

/// Internal possible calls. This is exclusively used
/// by the SequencerHandler
enum Calls {
    ClusterState(Call<(), ClusterState>),
    /// executed commands
    EnqueueBatch(Call<Arc<[Record]>, Result<LogletCommit, SequencerError>>),
}

/// Main interaction interface with the sequencer state machine
#[derive(Debug, Clone)]
pub struct SequencerHandle {
    /// internal commands channel.
    commands: mpsc::Sender<Calls>,
    global: Arc<SequencerGlobalState>,
}

pub(crate) struct SequencerHandleSink {
    commands: mpsc::Receiver<Calls>,
}

impl SequencerHandle {
    pub(crate) fn pair(
        global: Arc<SequencerGlobalState>,
    ) -> (SequencerHandle, SequencerHandleSink) {
        // todo: the size of the channel should be 1
        let (commands_sender, commands_receiver) = mpsc::channel::<Calls>(1);
        (
            SequencerHandle {
                commands: commands_sender,
                global,
            },
            SequencerHandleSink {
                commands: commands_receiver,
            },
        )
    }

    pub fn watch_tail(&self) -> futures::stream::BoxStream<'static, TailState<LogletOffset>> {
        Box::pin(self.global.committed_tail.to_stream())
    }

    pub async fn cluster_state(&self) -> Result<ClusterState, ShutdownError> {
        let (receiver, command) = Call::from_request(());
        self.commands
            .send(Calls::ClusterState(command))
            .await
            .map_err(|_| ShutdownError)?;

        receiver.await.map_err(|_| ShutdownError)
    }

    pub async fn enqueue_batch(
        &self,
        payloads: Arc<[Record]>,
    ) -> Result<LogletCommit, SequencerError> {
        let (receiver, command) = Call::from_request(payloads);
        self.commands
            .send(Calls::EnqueueBatch(command))
            .await
            .map_err(|_| ShutdownError)?;

        receiver.await.map_err(|_| ShutdownError)?
    }
}

#[derive(Clone, Debug)]
pub struct ClusterState {
    pub sequencer_id: GenerationalNodeId,
    pub global_committed_tail: TailState<LogletOffset>,
}

/// Sequencer inner state machine
///
/// this holds for example, the replica set (log servers)
/// information about global tail, etc...
struct SequencerInner {
    global: Arc<SequencerGlobalState>,
    log_server_manager: RemoteLogServerManager,
    selector: SpreadSelector,
    metadata: Metadata,
    first_offset: LogletOffset,
    batch_permits: Arc<Semaphore>,
    response_tracker: ResponseTracker<Stored>,
}

pub struct Sequencer;
impl Sequencer {
    pub fn start(
        node_id: GenerationalNodeId,
        loglet_id: ReplicatedLogletId,
        node_set: NodeSet,
        selector: SpreadSelector,
        metadata: Metadata,
        networking: Networking,
        max_inflight_batches: usize,
        router_builder: &mut MessageRouterBuilder,
    ) -> Result<SequencerHandle, SequencerError> {
        // - register for all potential response streams from the log-server(s).

        // shared state with appenders
        let global = Arc::new(SequencerGlobalState {
            node_id,
            loglet_id,
            committed_tail: TailOffsetWatch::new(TailState::Open(LogletOffset::OLDEST)),
        });

        // create a command channel to be used by the sequencer handler. The handler then can be used
        // to call and execute commands on the sequencer directly
        let (handle, sink) = SequencerHandle::pair(Arc::clone(&global));

        let response_tracker = ResponseTracker::default();
        router_builder.add_message_handler(response_tracker.clone());

        let log_server_manager =
            RemoteLogServerManager::new(loglet_id, metadata.clone(), networking, node_set);

        let sequencer = SequencerInner {
            global,
            log_server_manager,
            selector,
            metadata,
            first_offset: LogletOffset::OLDEST,
            batch_permits: Arc::new(Semaphore::new(max_inflight_batches)),
            response_tracker,
        };

        task_center().spawn_unmanaged(
            TaskKind::SystemService,
            "leader-sequencer",
            None,
            sequencer.run(sink),
        )?;

        Ok(handle)
    }
}

impl SequencerInner {
    async fn run(mut self, mut sink: SequencerHandleSink) {
        let shutdown = cancellation_watcher();
        tokio::pin!(shutdown);

        // enter main state machine loop
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => {
                    break;
                },
                Some(command) = sink.commands.recv() => {
                    self.process_command(command).await;
                }
            }
        }
    }

    /// process calls from the SequencerHandler.
    async fn process_command(&mut self, command: Calls) {
        match command {
            Calls::ClusterState(command) => {
                let Call { sender, .. } = command;
                let _ = sender.send(self.cluster_state());
            }
            Calls::EnqueueBatch(command) => {
                let Call { request, sender } = command;

                let _ = sender.send(self.enqueue_batch(request).await);
            }
        }
    }

    fn cluster_state(&self) -> ClusterState {
        ClusterState {
            global_committed_tail: self.global.committed_tail().get().to_owned(),
            sequencer_id: self.global.node_id,
        }
    }

    async fn enqueue_batch(
        &mut self,
        records: Arc<[Record]>,
    ) -> Result<LogletCommit, SequencerError> {
        if self.global.committed_tail().is_sealed() {
            // todo: (question) do we return a sealed loglet commit, or error.
            return Ok(LogletCommit::sealed());
        }

        let permit = self.batch_permits.clone().acquire_owned().await.unwrap();

        let next_first_offset = records.tail(self.first_offset)?;

        let (commit, resolver) = LogletCommit::deferred();

        let appender = Appender::new(
            Arc::clone(&self.global),
            self.log_server_manager.clone(),
            self.response_tracker.clone(),
            self.selector.clone(),
            self.metadata.clone(),
            self.first_offset,
            records,
            permit,
            resolver,
        );

        task_center().spawn_unmanaged(TaskKind::Disposable, "appender", None, appender.run())?;
        self.first_offset = next_first_offset;

        Ok(commit)
    }
}

trait BatchExt {
    /// tail computes inflight tail after this batch is committed
    fn tail(&self, first_offset: LogletOffset) -> Result<LogletOffset, SequencerError>;
}

impl BatchExt for Arc<[Record]> {
    fn tail(&self, first_offset: LogletOffset) -> Result<LogletOffset, SequencerError> {
        let len = u32::try_from(self.len()).map_err(|_| SequencerError::InvalidBatchLength)?;

        first_offset
            .checked_add(len)
            .map(LogletOffset::from)
            .ok_or(SequencerError::LogletOffsetExhausted)
    }
}
