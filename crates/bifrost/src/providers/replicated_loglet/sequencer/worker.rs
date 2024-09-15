use std::{
    collections::{HashSet, VecDeque},
    sync::{Arc, Weak},
};

use restate_core::{cancellation_token, ShutdownError, TaskCenter, TaskKind};
use tokio::sync::mpsc;

use restate_types::{
    logs::{LogletOffset, Record, SequenceNumber},
    net::log_server::{Store, StoreFlags},
    GenerationalNodeId,
};

use crate::loglet::Resolver;

use super::{
    node::{Node, NodeClient},
    SequencerGlobalState,
};

struct BatchQueue {
    queue: VecDeque<Batch>,
}

#[derive(Debug)]
pub(crate) struct Payload {
    pub first_offset: LogletOffset,
    pub records: Arc<[Record]>,
}

impl Payload {
    pub fn inflight_tail(&self) -> Option<LogletOffset> {
        let len = u32::try_from(self.records.len()).ok()?;
        self.first_offset.checked_add(len).map(Into::into)
    }
}

#[derive(derive_more::Debug)]
pub(crate) struct Batch {
    pub payload: Arc<Payload>,
    pub spread: HashSet<GenerationalNodeId>,
    #[debug(ignore)]
    pub resolver: Resolver,
}

pub(crate) type SendPermit<'a> = mpsc::Permit<'a, Weak<Payload>>;

#[derive(Clone, Debug)]
pub(crate) struct NodeWorkerHandle {
    sender: mpsc::Sender<Weak<Payload>>,
}

impl NodeWorkerHandle {
    /// reserve a send slot on the worker queue
    pub async fn reserve(&self) -> Result<SendPermit, mpsc::error::SendError<()>> {
        self.sender.reserve().await
    }
}

pub(crate) struct NodeWorker<C> {
    batch_receiver: mpsc::Receiver<Weak<Payload>>,
    node: Node<C>,
    global: Arc<SequencerGlobalState>,
}

impl<C> NodeWorker<C>
where
    C: NodeClient + Send + Sync + 'static,
{
    pub fn start(
        tc: &TaskCenter,
        node: Node<C>,
        queue_size: usize,
        shared: Arc<SequencerGlobalState>,
    ) -> Result<NodeWorkerHandle, ShutdownError> {
        let (batch_sender, batch_receiver) = mpsc::channel(queue_size);
        let handle = NodeWorkerHandle {
            sender: batch_sender,
        };

        let worker = NodeWorker {
            batch_receiver,
            node,
            global: shared,
        };

        tc.spawn_unmanaged(TaskKind::Disposable, "appender", None, worker.run())?;

        Ok(handle)
    }

    async fn run(mut self) {
        let token = cancellation_token();
        loop {
            tokio::select! {
                _ = token.cancelled() => {
                    return;
                }
                Some(batch) = self.batch_receiver.recv() => {
                    self.process_batch(batch).await;
                }
            }
        }
    }

    async fn process_batch(&mut self, batch: Weak<Payload>) {
        let batch = match batch.upgrade() {
            Some(batch) => batch,
            None => return,
        };

        let inflight_tail = batch.first_offset + batch.records.len() as u32;
        if inflight_tail <= self.global.committed_tail() {
            // todo: (question) batch is already committed and we can safely ignore it?
            return;
        }

        let store = Store {
            first_offset: batch.first_offset,
            flags: StoreFlags::empty(),
            known_archived: LogletOffset::INVALID,
            known_global_tail: self.global.committed_tail(),
            loglet_id: self.global.loglet_id,
            sequencer: self.global.node_id,
            timeout_at: None,
            // todo: (question) better way to do this?
            payloads: Vec::from_iter(batch.records.iter().map(|r| r.clone())),
        };

        if let Err(err) = self.node.client().enqueue_store(store).await {
            //todo: retry
            tracing::error!(error = %err, "failed to send store to node");
        }
    }
}
