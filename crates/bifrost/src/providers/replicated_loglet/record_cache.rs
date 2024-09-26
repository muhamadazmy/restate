// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{future::Future, sync::Arc};

use restate_core::network::NetworkError;
use restate_types::{
    logs::{LogletOffset, Record},
    replicated_loglet::ReplicatedLogletId,
};

/// A placeholder for a global record cache.
///
/// This can be safely shared between all ReplicatedLoglet(s) and the LocalSequencers or the
/// RemoteSequencers
///
///
/// This is a streaming LRU-cache with total memory budget tracking and enforcement.
#[derive(Clone, Debug)]
pub struct RecordCache<R = DefaultRecordFetcher> {
    inner: R,
}

impl RecordCache<DefaultRecordFetcher> {
    pub fn new(_memory_budget_bytes: usize) -> Self {
        Self::with_record_fetcher(_memory_budget_bytes, DefaultRecordFetcher)
    }
}

impl<R> RecordCache<R>
where
    R: RecordFetch,
{
    pub fn with_record_fetcher(_memory_budget_bytes: usize, fetch: R) -> Self {
        Self { inner: fetch }
    }

    /// Writes a record to cache externally
    pub fn push(_loglet_id: ReplicatedLogletId, _offset: LogletOffset, _record: Record) {
        todo!()
    }

    /// Extend cache with records
    pub fn extend<I: AsRef<[Record]>>(
        &self,
        _loglet_id: ReplicatedLogletId,
        _first_offset: LogletOffset,
        _records: I,
    ) {
        todo!()
    }

    /// Get a for given loglet id and offset.
    pub async fn get(
        &self,
        _loglet_id: ReplicatedLogletId,
        _offset: ReplicatedLogletId,
    ) -> Option<Record> {
        // - check cache for record.
        // - if not found, check self.inner
        // - if found, update cache, and return
        // - if not found, return None.

        todo!()
    }
}

#[derive(thiserror::Error, Debug)]
pub enum CacheError {
    #[error("record not found")]
    NotFound,

    #[error(transparent)]
    Network(NetworkError),
}

/// RecordFetch fetches record from a "remote" source
pub trait RecordFetch {
    fn fetch(
        &self,
        loglet_id: ReplicatedLogletId,
        offset: LogletOffset,
    ) -> impl Future<Output = Result<Arc<[Record]>, CacheError>>;
}

#[derive(Clone)]
pub struct DefaultRecordFetcher;

impl RecordFetch for DefaultRecordFetcher {
    fn fetch(
        &self,
        _loglet_id: ReplicatedLogletId,
        _offset: LogletOffset,
    ) -> impl Future<Output = Result<Arc<[Record]>, CacheError>> {
        async { Err(CacheError::NotFound) }
    }
}
