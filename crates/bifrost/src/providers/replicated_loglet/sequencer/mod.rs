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

use restate_types::{replicated_loglet::ReplicatedLogletId, GenerationalNodeId};

use crate::loglet::util::TailOffsetWatch;

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
