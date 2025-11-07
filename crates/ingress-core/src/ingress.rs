// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{collections::HashMap, sync::Arc};

use tokio::sync::Semaphore;

use restate_core::{
    network::{Networking, TransportConnect},
    partitions::PartitionRouting,
};
use restate_types::{
    identifiers::{PartitionId, WithPartitionKey},
    live::Live,
    net::ingress::IngestRecord,
    partitions::{FindPartition, PartitionTable, PartitionTableError},
};

use crate::{
    RecordCommit, SessionOptions,
    session::{SessionHandle, SessionManager},
};

#[derive(Debug, thiserror::Error)]
pub enum IngestionError {
    #[error("Ingress closed")]
    Closed,
    #[error(transparent)]
    PartitionTableError(#[from] PartitionTableError),
}

#[derive(Clone)]
pub struct Ingress<T> {
    manager: SessionManager<T>,
    partition_table: Live<PartitionTable>,
    // budget for inflight invocations.
    // this should be a memory budget but it's
    // not possible atm to compute the serialization
    // size of an invocation.
    permits: Arc<Semaphore>,

    handlers: HashMap<PartitionId, SessionHandle>,
}

impl<T> Ingress<T> {
    pub fn new(
        networking: Networking<T>,
        partition_table: Live<PartitionTable>,
        partition_routing: PartitionRouting,
        budget: usize,
        opts: Option<SessionOptions>,
    ) -> Self {
        Self {
            manager: SessionManager::new(networking, partition_routing, opts),
            partition_table,
            permits: Arc::new(Semaphore::new(budget)),
            handlers: HashMap::default(),
        }
    }
}

impl<T> Ingress<T>
where
    T: TransportConnect,
{
    pub async fn ingest(&mut self, record: IngestRecord) -> Result<RecordCommit, IngestionError> {
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| IngestionError::Closed)?;

        let partition_key = record.invocation.partition_key();
        let partition_id = self
            .partition_table
            .pinned()
            .find_partition_id(partition_key)?;

        let handle = self
            .handlers
            .entry(partition_id)
            .or_insert_with(|| self.manager.get(partition_id));

        handle
            .ingest(permit, record)
            .map_err(|_| IngestionError::Closed)
    }

    /// Once closed, calls to ingest will return [`IngestionError::Closed`].
    /// Inflight records might still get committed.
    pub fn close(&self) {
        self.permits.close();
        self.manager.close();
    }
}
