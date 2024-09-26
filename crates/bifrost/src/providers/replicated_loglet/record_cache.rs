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

use futures::stream::BoxStream;
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
    pub fn set(_loglet_id: ReplicatedLogletId, _offset: LogletOffset, _record: Record) {
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

    /// Returns the last record offset that was ever written to this cache.
    ///
    /// Note that this does not mean that the record still available in cache.
    /// The record could have been already evicted by the time you call a [`Self::get`]
    pub fn last_record(&self, _loglet_id: ReplicatedLogletId) -> Option<LogletOffset> {
        todo!()
    }

    /// Watch the cache for new new records that been
    /// added to cache
    ///
    /// This allows readers to get notified once new records are available in
    /// cache.
    ///
    /// [note]: This is probably not needed because the loglet can already provide
    /// a decent watch tail interface.
    pub fn watch_records(
        &self,
        _loglet_id: ReplicatedLogletId,
        _from: LogletOffset,
    ) -> BoxStream<LogletOffset> {
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
    /// fetch ahead record from backend. The fetch will try to
    /// return as many records it can fetch in one go as possible
    /// so cache is warmed up for next .get() calls
    ///
    /// this is to avoid range cache misses  
    fn fetch(
        &self,
        loglet_id: ReplicatedLogletId,
        offset: LogletOffset,
    ) -> impl Future<Output = Result<Arc<[Record]>, CacheError>>;
}

#[derive(Clone)]
pub struct DefaultRecordFetcher;

impl RecordFetch for DefaultRecordFetcher {
    async fn fetch(
        &self,
        _loglet_id: ReplicatedLogletId,
        _offset: LogletOffset,
    ) -> Result<Arc<[Record]>, CacheError> {
        Err(CacheError::NotFound)
    }
}
