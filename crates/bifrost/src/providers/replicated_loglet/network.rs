// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// todo(asoli): remove when fleshed out
#![allow(dead_code)]

use std::pin::Pin;
use std::sync::Arc;

use futures::{Stream, StreamExt};
use tracing::trace;

use restate_core::network::{Incoming, MessageRouterBuilder, TransportConnect};
use restate_core::{cancellation_watcher, task_center, Metadata, ShutdownError, TaskKind};
use restate_types::config::ReplicatedLogletOptions;
use restate_types::logs::{LogletOffset, SequenceNumber};
use restate_types::net::replicated_loglet::{
    Append, Appended, CommonRequestHeader, CommonResponseHeader, SequencerStatus,
};

use super::loglet::ReplicatedLoglet;
use super::provider::ReplicatedLogletProvider;
use crate::loglet::{AppendError, Loglet, OperationError};

type MessageStream<T> = Pin<Box<dyn Stream<Item = Incoming<T>> + Send + Sync + 'static>>;

pub struct RequestPump {
    metadata: Metadata,
    append_stream: MessageStream<Append>,
}

impl RequestPump {
    pub fn new(
        _opts: &ReplicatedLogletOptions,
        metadata: Metadata,
        router_builder: &mut MessageRouterBuilder,
    ) -> Self {
        // todo(asoli) read from opts
        let queue_length = 10;
        let append_stream = router_builder.subscribe_to_stream(queue_length);
        Self {
            metadata,
            append_stream,
        }
    }

    /// Must run in task-center context
    pub async fn run<T: TransportConnect>(
        mut self,
        provider: Arc<ReplicatedLogletProvider<T>>,
    ) -> anyhow::Result<()> {
        trace!("Starting replicated loglet request pump");
        let mut cancel = std::pin::pin!(cancellation_watcher());
        // provider.get_loglet(log_id, segment_index, params)
        loop {
            tokio::select! {
                _ = &mut cancel => {
                    break;
                }
                Some(append) = self.append_stream.next() => {
                    self.handle_append(Arc::clone(&provider), append).await;
                }
            }
        }

        Ok(())
    }

    /// Infailable handle_append method
    async fn handle_append<T: TransportConnect>(
        &mut self,
        provider: Arc<ReplicatedLogletProvider<T>>,
        incoming: Incoming<Append>,
    ) {
        let task = HandleAppendTask {
            metadata: self.metadata.clone(),
            provider,
        };

        // we spawn immediately to avoid slowing the request pump loop
        // in case responses will go over a slow channel
        let _ = task_center().spawn_child(
            TaskKind::Disposable,
            "handle-append-request",
            None,
            task.run(incoming),
        );
    }
}

struct HandleAppendTask<T> {
    provider: Arc<ReplicatedLogletProvider<T>>,
    metadata: Metadata,
}

impl<T> HandleAppendTask<T>
where
    T: TransportConnect,
{
    fn get_loglet(
        &self,
        provider: &ReplicatedLogletProvider<T>,
        header: &CommonRequestHeader,
    ) -> Result<Arc<ReplicatedLoglet<T>>, SequencerStatus> {
        let logs = self.metadata.logs();
        let chain = logs
            .chain(&header.log_id)
            .ok_or(SequencerStatus::BadLogletId)?;

        // appends are only possible to tail segment.
        let segment = chain
            .iter()
            .rev()
            .find(|segment| segment.index() == header.segment_index)
            .ok_or(SequencerStatus::BadLogletId)?;

        let loglet = provider
            .get_replicated_loglet(header.log_id, header.segment_index, &segment.config.params)
            .map_err(|err| SequencerStatus::Error(err.to_string()))?;

        Ok(loglet)
    }

    async fn run(self, request: Incoming<Append>) -> anyhow::Result<()> {
        let (reciprocal, append) = request.split();
        let cancelled = cancellation_watcher();

        let result = tokio::select! {
            _ = cancelled => {
                // we could try to respond to the request still
                // but it's probably too late.
                anyhow::bail!(ShutdownError);
            },
            result = self.append(&self.provider, append) => {
                result
            }
        };

        let appended = match result {
            Ok(appended) => appended,
            Err(err) => Appended {
                first_offset: LogletOffset::INVALID,
                header: CommonResponseHeader {
                    known_global_tail: None,
                    sealed: None,
                    status: err,
                },
            },
        };

        reciprocal.prepare(appended).send().await?;
        Ok(())
    }

    async fn append(
        &self,
        provider: &ReplicatedLogletProvider<T>,
        append: Append,
    ) -> Result<Appended, SequencerStatus> {
        let loglet = self.get_loglet(provider, &append.header)?;

        // We get the sequencer instead of calling loglet.enqueue_batch to make sure this
        // is the sequencer node. Otherwise, the client gets an error and should retry against
        // the sequencer node.
        if !loglet.is_sequencer_local() {
            return Err(SequencerStatus::NotSequencer);
        }

        let global_tail = loglet.known_global_tail();
        // This is okay to be done in scope, since it's the only way to exert some back pressure on the callers
        // until the sequencer has enough capacity to accept these records.
        // This call should return immediately once records are scheduled for commit.
        let loglet_commit = match loglet.enqueue_batch(append.payloads).await {
            Err(err) => {
                let appended = Appended {
                    header: CommonResponseHeader {
                        known_global_tail: Some(global_tail.latest_offset()),
                        sealed: Some(global_tail.is_sealed()),
                        status: match err {
                            OperationError::Shutdown(_) => SequencerStatus::Shutdown,
                            OperationError::Other(err) => SequencerStatus::Error(err.to_string()),
                        },
                    },
                    first_offset: LogletOffset::INVALID,
                };
                return Ok(appended);
            }
            Ok(loglet_commit) => loglet_commit,
        };

        let response = match loglet_commit.await {
            Ok(offset) => Appended {
                header: CommonResponseHeader {
                    known_global_tail: Some(global_tail.latest_offset()),
                    sealed: Some(global_tail.is_sealed()),
                    status: SequencerStatus::Ok,
                },
                first_offset: offset,
            },
            Err(AppendError::Sealed) => Appended {
                header: CommonResponseHeader {
                    known_global_tail: Some(global_tail.latest_offset()),
                    sealed: Some(global_tail.is_sealed()), // this must be true
                    status: SequencerStatus::Sealed,
                },
                first_offset: LogletOffset::INVALID,
            },
            Err(err) => Appended {
                header: CommonResponseHeader {
                    known_global_tail: Some(global_tail.latest_offset()),
                    sealed: Some(global_tail.is_sealed()),
                    status: SequencerStatus::Error(err.to_string()),
                },
                first_offset: LogletOffset::INVALID,
            },
        };

        Ok(response)
    }
}
