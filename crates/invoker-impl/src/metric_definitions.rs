// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::LazyLock;

use dashmap::{DashMap, Entry};
/// Optional to have but adds description/help message to the metrics emitted to
/// the metrics' sink.
use metrics::{Unit, describe_counter, describe_histogram};

use restate_types::identifiers::PartitionId;

/// Lazily initialized cache that maps partition ids to their string representation for metric labels,
/// avoiding fresh string allocations whenever a partition id is used as a metric dimension.
pub(crate) static PARTITION_ID_LOOKUP: LazyLock<PartitionLookup> =
    LazyLock::new(PartitionLookup::new);

pub(crate) struct PartitionLookup {
    index: Vec<String>,
    extra: DashMap<u16, String>,
}

impl PartitionLookup {
    const SIZE: u16 = 2048;

    fn new() -> Self {
        let mut index = Vec::with_capacity(Self::SIZE as usize);
        for i in 0..Self::SIZE {
            index.push(i.to_string());
        }

        Self {
            index,
            extra: DashMap::new(),
        }
    }

    #[inline]
    pub fn get(&'static self, id: PartitionId) -> &'static str {
        let id = u16::from(id);
        if id < Self::SIZE {
            // fast access
            return self.index[id as usize].as_str();
        }

        // somehow we are running more that SIZE partitions
        // it's a slower path but still better than doing a .to_string()
        // on each metric
        let entry = self.extra.entry(id);
        match entry {
            Entry::Occupied(entry) => unsafe {
                std::mem::transmute::<&str, &'static str>(entry.get())
            },
            Entry::Vacant(entry) => {
                let r = entry.insert(id.to_string());
                unsafe { std::mem::transmute::<&str, &'static str>(r.as_str()) }
            }
        }
    }
}

pub const INVOKER_ENQUEUE: &str = "restate.invoker.enqueue.total";
pub const INVOKER_INVOCATION_TASKS: &str = "restate.invoker.invocation_tasks.total";
pub const INVOKER_TASK_DURATION: &str = "restate.invoker.task_duration.seconds";

pub const TASK_OP_STARTED: &str = "started";
pub const TASK_OP_SUSPENDED: &str = "suspended";
pub const TASK_OP_FAILED: &str = "failed";
pub const TASK_OP_COMPLETED: &str = "completed";

pub(crate) fn describe_metrics() {
    describe_counter!(
        INVOKER_ENQUEUE,
        Unit::Count,
        "Number of invocations that were added to the queue"
    );

    describe_counter!(
        INVOKER_INVOCATION_TASKS,
        Unit::Count,
        "Invocation task operation"
    );

    describe_histogram!(
        INVOKER_TASK_DURATION,
        Unit::Seconds,
        "Time taken to complete an invoker task"
    );
}
