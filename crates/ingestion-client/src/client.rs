// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{
    cmp::Ordering,
    num::NonZeroUsize,
    sync::{Arc, atomic::AtomicUsize},
};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use restate_core::{
    network::{Networking, TransportConnect},
    partitions::PartitionRouting,
};
use restate_types::{
    identifiers::PartitionKey,
    live::Live,
    net::ingress::IngestRecord,
    partitions::{FindPartition, PartitionTable, PartitionTableError},
};

use crate::{RecordCommit, SessionOptions, session::SessionManager};

/// Errors that can be observed when interacting with the ingress facade.
#[derive(Debug, thiserror::Error)]
pub enum IngestionError {
    #[error("Ingress closed")]
    Closed,
    #[error(transparent)]
    PartitionTableError(#[from] PartitionTableError),
}

/// High-level ingestion entry point that allocates permits and hands out session handles per partition.
/// [`IngestionClient`] can be cloned and shared across different routines. All users will share the same budget
/// and underlying partition sessions.
#[derive(Clone)]
pub struct IngestionClient<T> {
    manager: SessionManager<T>,
    partition_table: Live<PartitionTable>,
    // memory budget for inflight invocations.
    permits: Arc<Semaphore>,
    available_permits: Arc<AtomicUsize>,
}

impl<T> IngestionClient<T> {
    /// Builds a new ingestion facade with the provided networking stack, partition metadata, and
    /// budget (in bytes) for inflight records.
    pub fn new(
        networking: Networking<T>,
        partition_table: Live<PartitionTable>,
        partition_routing: PartitionRouting,
        memory_budget: NonZeroUsize,
        opts: Option<SessionOptions>,
    ) -> Self {
        Self {
            manager: SessionManager::new(networking, partition_routing, opts),
            partition_table,
            permits: Arc::new(Semaphore::new(memory_budget.get())),
            available_permits: Arc::new(AtomicUsize::new(memory_budget.get())),
        }
    }

    pub fn ensure_enough_permits(&self, required: usize) {
        use std::sync::atomic::Ordering;
        let mut available = self.available_permits.load(Ordering::Relaxed);
        while available < required {
            let delta = required - available;
            match self.available_permits.compare_exchange(
                available,
                required,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.permits.add_permits(delta);
                }
                Err(current) => {
                    available = current;
                }
            }
        }
    }
}

impl<T> IngestionClient<T>
where
    T: TransportConnect,
{
    /// Reserve `size` budget to send a record.
    pub async fn reserve(&self, size: u32) -> Result<IngestionPermit<'_, T>, IngestionError> {
        self.ensure_enough_permits(size as usize);

        let permit = self
            .permits
            .clone()
            .acquire_many_owned(size)
            .await
            .map_err(|_| IngestionError::Closed)?;

        Ok(IngestionPermit {
            permit,
            client: self,
        })
    }

    /// Ingest a record with `partition_key`.
    pub async fn ingest(
        &self,
        partition_key: PartitionKey,
        record: impl Into<IngestRecord>,
    ) -> Result<RecordCommit, IngestionError> {
        let record = record.into();

        self.reserve(record.estimate_size() as u32)
            .await?
            .ingest(partition_key, record)
    }

    /// Once closed, calls to ingest will return [`IngestionError::Closed`].
    /// Inflight records might still get committed.
    pub fn close(&self) {
        self.permits.close();
        self.manager.close();
    }
}

pub struct IngestionPermit<'a, T> {
    permit: OwnedSemaphorePermit,
    client: &'a IngestionClient<T>,
}

impl<'a, T> IngestionPermit<'a, T>
where
    T: TransportConnect,
{
    /// Consume the reserved budget to send a record.
    ///
    /// Panics: If record size is bigger than reserved budget
    pub fn ingest(
        mut self,
        partition_key: PartitionKey,
        record: impl Into<IngestRecord>,
    ) -> Result<RecordCommit, IngestionError> {
        let record = record.into();

        let size = record.estimate_size();
        match size.cmp(&self.permit.num_permits()) {
            Ordering::Equal => {}
            Ordering::Greater => {
                // While this is a programming error
                // we still can handle this case.
                let _ = self.permit.split(size - self.permit.num_permits());
            }
            Ordering::Less => {
                // I think it's safe to assume this is a programming error
                // it's up to the developer to make sure to use the correct
                // record
                panic!("Record size is bigger than reserved permits");
            }
        };

        let partition_id = self
            .client
            .partition_table
            .pinned()
            .find_partition_id(partition_key)?;

        self.client
            .manager
            .get(partition_id)
            .ingest(self.permit, record)
            .map_err(|_| IngestionError::Closed)
    }
}

#[cfg(test)]
mod test {
    use std::{num::NonZeroUsize, time::Duration};

    use bytes::Bytes;
    use futures::{FutureExt, StreamExt};
    use googletest::prelude::*;
    use test_log::test;

    use restate_core::{
        Metadata, TaskCenter, TestCoreEnvBuilder,
        network::{
            BackPressureMode, FailingConnector, Incoming, Rpc, ServiceMessage, ServiceStream,
        },
        partitions::PartitionRouting,
    };
    use restate_types::{
        Version,
        identifiers::{LeaderEpoch, PartitionId},
        logs::Keys,
        net::{
            self, RpcRequest,
            ingress::{IngestRecord, IngestResponse, ReceivedIngestRequest},
            partition_processor::PartitionLeaderService,
        },
        partitions::{
            PartitionTable,
            state::{LeadershipState, PartitionReplicaSetStates},
        },
        retries::RetryPolicy,
    };

    use crate::{CommitError, IngestionClient, SessionOptions};

    async fn init_env(
        batch_size: usize,
    ) -> (
        ServiceStream<PartitionLeaderService>,
        IngestionClient<FailingConnector>,
    ) {
        let mut builder = TestCoreEnvBuilder::with_incoming_only_connector()
            .add_mock_nodes_config()
            .set_partition_table(PartitionTable::with_equally_sized_partitions(
                Version::MIN,
                4,
            ));

        let partition_replica_set_states = PartitionReplicaSetStates::default();
        for i in 0..4 {
            partition_replica_set_states.note_observed_leader(
                i.into(),
                LeadershipState {
                    current_leader: builder.my_node_id,
                    current_leader_epoch: LeaderEpoch::INITIAL,
                },
            );
        }

        let svc = builder
            .router_builder
            .register_service::<net::partition_processor::PartitionLeaderService>(
                10,
                BackPressureMode::PushBack,
            );

        let incoming = svc.start();

        let env = builder.build().await;
        let client = IngestionClient::new(
            env.networking,
            env.metadata.updateable_partition_table(),
            PartitionRouting::new(partition_replica_set_states, TaskCenter::current()),
            NonZeroUsize::new(10 * 1024 * 1024).unwrap(), // 10MB
            SessionOptions {
                batch_size,
                connect_retry_policy: RetryPolicy::fixed_delay(Duration::from_millis(10), None),
            }
            .into(),
        );

        (incoming, client)
    }

    async fn must_next(
        recv: &mut ServiceStream<PartitionLeaderService>,
    ) -> Incoming<Rpc<ReceivedIngestRequest>> {
        let Some(ServiceMessage::Rpc(msg)) = recv.next().await else {
            panic!("stream closed");
        };

        assert_eq!(msg.msg_type(), ReceivedIngestRequest::TYPE);
        msg.into_typed()
    }

    #[test(restate_core::test)]
    async fn test_client_single_record() {
        let (mut incoming, client) = init_env(10).await;

        let commit = client
            .ingest(
                0,
                IngestRecord {
                    keys: Keys::None,
                    record: Bytes::from("hello world"),
                },
            )
            .await
            .unwrap();

        let msg = must_next(&mut incoming).await;
        let (rx, body) = msg.split();
        assert_that!(
            body.records,
            all!(
                len(eq(1)),
                contains(predicate(|v: &IngestRecord| v.record == "hello world"))
            )
        );

        rx.send(IngestResponse::Ack);

        commit.await.expect("to resolve");
    }

    #[test(restate_core::test)]
    async fn test_client_single_record_retry() {
        let (mut incoming, client) = init_env(10).await;

        let mut commit = client
            .ingest(
                0,
                IngestRecord {
                    keys: Keys::None,
                    record: Bytes::from("hello world"),
                },
            )
            .await
            .unwrap();

        let msg = must_next(&mut incoming).await;
        let (rx, _) = msg.split();
        rx.send(IngestResponse::NotLeader { of: 0.into() });

        assert!((&mut commit).now_or_never().is_none());

        // ingress will retry automatically so we must receive another message
        let msg = must_next(&mut incoming).await;
        let (rx, body) = msg.split();
        assert_that!(
            body.records,
            all!(
                len(eq(1)),
                contains(predicate(|v: &IngestRecord| v.record == "hello world"))
            )
        );
        // lets acknowledge it this time
        rx.send(IngestResponse::Ack);

        commit.await.expect("to resolve");
    }

    #[test(restate_core::test)]
    async fn test_client_close() {
        let (_, client) = init_env(10).await;

        let commit = client
            .ingest(
                0,
                IngestRecord {
                    keys: Keys::None,
                    record: Bytes::from("hello world"),
                },
            )
            .await
            .unwrap();

        client.close();

        assert!(matches!(commit.await, Err(CommitError::Cancelled)));
    }

    #[test(restate_core::test(start_paused = true))]
    async fn test_client_dispatch() {
        let (mut incoming, client) = init_env(10).await;

        let pt = Metadata::with_current(|p| p.partition_table_snapshot());

        for p in 0..4 {
            let partition_id = PartitionId::from(p);
            let partition = pt.get(&partition_id).unwrap();
            client
                .ingest(
                    *partition.key_range.start(),
                    IngestRecord {
                        keys: Keys::None,
                        record: Bytes::from(format!("partition {p}")),
                    },
                )
                .await
                .unwrap();
        }

        tokio::time::advance(Duration::from_millis(10)).await; // batch timeout

        // what happens is that we still get 4 different message because each targets
        // a single partition.
        let mut received = vec![];
        for _ in 0..4 {
            let msg = must_next(&mut incoming).await;
            received.push(msg.sort_code());
        }

        assert_that!(
            received,
            all!(
                len(eq(4)), //4 messages for 4 partitions
                contains(eq(Some(0))),
                contains(eq(Some(1))),
                contains(eq(Some(2))),
                contains(eq(Some(3))),
            )
        );
    }
}
