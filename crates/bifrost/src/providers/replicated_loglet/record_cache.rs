// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::RangeBounds;

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
pub struct RecordCache {}

impl RecordCache {
    pub fn new(_memory_budget_bytes: usize) -> Self {
        Self {}
    }

    /// Writes a record to cache.
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
    pub fn get(
        &self,
        _loglet_id: ReplicatedLogletId,
        _offset: ReplicatedLogletId,
    ) -> Option<&Record> {
        todo!()
    }

    /// Get range of records from cache.
    pub fn get_range<R>(&self, _loglet_id: ReplicatedLogletId, _range: R) -> Vec<CachedRecords>
    where
        R: RangeBounds<LogletOffset>,
    {
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
        records: Vec<Record>,
    },
}
