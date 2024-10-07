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

use std::cmp::Ordering;
use std::pin::Pin;
use std::sync::Arc;

use dashmap::{DashMap, Entry};
use futures::{Stream, StreamExt};
use tracing::trace;

use restate_core::network::{Incoming, MessageRouterBuilder, Reciprocal, TransportConnect};
use restate_core::{cancellation_watcher, task_center, Metadata, TaskKind};
use restate_types::config::ReplicatedLogletOptions;
use restate_types::logs::{LogletOffset, SequenceNumber};
use restate_types::net::replicated_loglet::{
    Append, Appended, CommonRequestHeader, CommonResponseHeader, SequencerStatus,
};
use restate_types::replicated_loglet::ReplicatedLogletId;

use super::loglet::ReplicatedLoglet;
use super::provider::ReplicatedLogletProvider;
use crate::loglet::util::TailOffsetWatch;
use crate::loglet::{AppendError, LogletCommit};

type MessageStream<T> = Pin<Box<dyn Stream<Item = Incoming<T>> + Send + Sync + 'static>>;

pub struct RequestPump<T> {
    metadata: Metadata,
    append_stream: MessageStream<Append>,
    active_loglet: DashMap<ReplicatedLogletId, Arc<ReplicatedLoglet<T>>>,
}

impl<T> RequestPump<T>
where
    T: TransportConnect,
{
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
            active_loglet: DashMap::default(),
        }
    }

    /// Must run in task-center context
    pub async fn run(mut self, provider: Arc<ReplicatedLogletProvider<T>>) -> anyhow::Result<()> {
        trace!("Starting replicated loglet request pump");
        let mut cancel = std::pin::pin!(cancellation_watcher());
        // provider.get_loglet(log_id, segment_index, params)
        loop {
            tokio::select! {
                _ = &mut cancel => {
                    break;
                }
                Some(append) = self.append_stream.next() => {
                    self.handle_append(&provider, append).await;
                }
            }
        }
        Ok(())
    }

    /// Returns the ReplicatedLoglet iff the requested segment
    /// is the latest segment in the chain. Since this is mainly used for appends,
    /// it's only valid to append to latest `open` segment of the chain.
    ///
    /// **Note**: Since once a loglet is `available` it will be cached
    /// forever it's possible that this function still returns a ReplicatedLoglet
    /// that is __not__ the last segment of the chain. If that happens,
    /// the sequencer will still reply with a valid `Sealed` response.
    async fn get_latest_segment(
        &self,
        provider: &ReplicatedLogletProvider<T>,
        header: &CommonRequestHeader,
    ) -> Result<Arc<ReplicatedLoglet<T>>, SequencerStatus> {
        match self.active_loglet.entry(header.loglet_id) {
            Entry::Occupied(entry) => Ok(entry.get().clone()),
            Entry::Vacant(entry) => {
                let logs = self.metadata.logs();
                let chain = logs
                    .chain(&header.log_id)
                    .ok_or(SequencerStatus::Malformed)?;

                // appends are only possible to tail segment.
                let segment = chain.tail();
                match header.segment_index.cmp(&segment.index()) {
                    Ordering::Less => {
                        // question(azmy): if the append message contains an `old`
                        // index is it okay to assume that segment is sealed.
                        return Err(SequencerStatus::Sealed);
                    }
                    Ordering::Greater => return Err(SequencerStatus::OutOfBounds),
                    Ordering::Equal => {
                        // just right!
                    }
                };
                let loglet = provider
                    .get_replicated_loglet(
                        header.log_id,
                        header.segment_index,
                        &segment.config.params,
                    )
                    .await
                    .map_err(|err| SequencerStatus::Error(err.to_string()))?;

                entry.insert(Arc::clone(&loglet));

                Ok(loglet)
            }
        }
    }

    /// Infailable handle_append method
    async fn handle_append(
        &mut self,
        provider: &ReplicatedLogletProvider<T>,
        incoming: Incoming<Append>,
    ) {
        if let Err(err) = self.append(provider, &incoming).await {
            let response = Appended {
                first_offset: LogletOffset::INVALID,
                header: CommonResponseHeader {
                    known_global_tail: None,
                    sealed: None,
                    status: err,
                },
            };

            let _ = incoming.into_outgoing(response).send().await;
        }
    }

    async fn append(
        &self,
        provider: &ReplicatedLogletProvider<T>,
        append: &Incoming<Append>,
    ) -> Result<(), SequencerStatus> {
        let loglet = self
            .get_latest_segment(provider, &append.body().header)
            .await?;

        // We get the sequencer instead of calling loglet.enqueue_batch to make sure this
        // is the sequencer node. Otherwise, the client gets an error and should retry against
        // the sequencer node.
        let sequencer = loglet.sequencer().ok_or(SequencerStatus::NotSequencer)?;

        let global_tail = sequencer.sequencer_state().global_committed_tail();
        // This is okay to be done in scope, since it's the only way to exert some back pressure on the callers
        // until the sequencer has enough capacity to accept these records.
        // This call should return immediately once records are scheduled for commit.
        let loglet_commit = match sequencer
            .enqueue_batch(Arc::clone(&append.body().payloads))
            .await
        {
            Err(err) => {
                let tail = *global_tail.get();
                let appended = Appended {
                    header: CommonResponseHeader {
                        known_global_tail: Some(tail.offset()),
                        sealed: Some(tail.is_sealed()),
                        status: SequencerStatus::Error(err.to_string()),
                    },
                    first_offset: LogletOffset::INVALID,
                };
                let _ = append.create_reciprocal().prepare(appended).send().await;
                return Ok(());
            }
            Ok(loglet_commit) => loglet_commit,
        };

        // Once records are accepted, we then can wait for the commit to happen in the background.
        let _ = task_center().spawn_unmanaged(
            TaskKind::Disposable,
            "request-pump-wait-commit",
            None,
            Self::wait_commit(
                append.create_reciprocal(),
                loglet_commit,
                global_tail.clone(),
            ),
        );

        Ok(())
    }

    async fn wait_commit(
        reciprocal: Reciprocal,
        loglet_commit: LogletCommit,
        global_tail: TailOffsetWatch,
    ) {
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
                    sealed: Some(global_tail.is_sealed()), // this must be true
                    status: SequencerStatus::Error(err.to_string()),
                },
                first_offset: LogletOffset::INVALID,
            },
        };

        let _ = reciprocal.prepare(response).send().await;
    }
}
