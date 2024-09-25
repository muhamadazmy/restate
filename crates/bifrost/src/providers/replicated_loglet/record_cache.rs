// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{ops::RangeBounds, sync::Arc};

use futures::stream::BoxStream;
use restate_types::{
    logs::{LogletOffset, Record},
    replicated_loglet::ReplicatedLogletId,
};

#[derive(thiserror::Error, Debug)]
pub enum RecordCacheError {
    #[error("Append offset is unexpected")]
    UnexpectedOffset,
}
/// A placeholder for a global record cache.
///
/// This can be safely shared between all ReplicatedLoglet(s) and the LocalSequencers or the
/// RemoteSequencers
///
///
/// This is a streaming LRU-cache with total memory budget tracking and enforcement.
#[derive(Clone, Debug)]
pub struct RecordCache {}

impl RecordCache {
    pub fn new(_memory_budget_bytes: usize) -> Self {
        Self {}
    }

    /// Append the next batch to the loglet_id cache.
    ///
    /// Cache expecting offsets to come in order. If the provided
    /// `first_offset` would create a gap in the cache the append
    /// will be rejected with an error.
    pub(crate) fn append_records(
        &self,
        _loglet_id: ReplicatedLogletId,
        _first_offset: LogletOffset,
        _records: Arc<[Record]>,
    ) -> Result<(), RecordCacheError> {
        todo!()
    }

    /// Release offset (inclusive) to readers.
    ///
    /// All active reader streams should receive all released batches.
    pub(crate) fn release_records(&self, _loglet_id: ReplicatedLogletId, _offset: LogletOffset) {
        todo!()
    }

    /// Read a range of records.
    ///
    /// A Gap can only appear as a first item in the returned
    /// range.
    ///
    /// It's up the the caller to full fill the gap by other means
    /// for example by reading directly from the log servers.
    pub fn read_range<R>(&self, _range: R) -> Vec<CachedRecords>
    where
        R: RangeBounds<LogletOffset>,
    {
        todo!()
    }

    /// Gets a read stream that starts from the given offset.
    ///
    /// The first item returned by the stream can be a gap
    /// if the `from` offset is not available in the cache
    /// anymore.
    pub fn get_read_stream(
        &self,
        _loglet_id: ReplicatedLogletId,
        _from: LogletOffset,
    ) -> BoxStream<CachedRecords> {
        todo!()
    }
}

pub enum CachedRecords {
    Gap {
        from: LogletOffset,
        to: LogletOffset,
    },
    Batch {
        first_offset: LogletOffset,
        records: Arc<[Record]>,
    },
}
