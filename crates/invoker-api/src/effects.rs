// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use restate_memory::NonZeroByteCount;
use restate_types::deployment::PinnedDeployment;
use restate_types::errors::InvocationError;
use restate_types::identifiers::InvocationId;
use restate_types::journal::EntryIndex;
use restate_types::journal::enriched::EnrichedRawEntry;
use restate_types::journal_events::raw::RawEvent;
use restate_types::journal_v2;
use restate_types::journal_v2::CommandIndex;
use restate_types::journal_v2::raw::RawEntry;
use restate_types::storage::{StoredRawEntry, StoredRawEntryHeader};
use restate_types::time::MillisSinceEpoch;

use crate::EffectKind::JournalEntryV2;

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Effect {
    pub invocation_id: InvocationId,
    // removed in v1.6
    // #[cfg_attr(
    //     feature = "serde",
    //     serde(default, skip_serializing_if = "num_traits::Zero::is_zero")
    // )]
    // pub invocation_epoch: InvocationEpoch,
    pub kind: EffectKind,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
// todo: fix this and box the large variant (EffectKind is 320 bytes)
#[allow(clippy::large_enum_variant)]
pub enum EffectKind {
    /// This is sent before any new entry is created by the invoker.
    /// This won't be sent if the deployment_id is already set.
    PinnedDeployment(PinnedDeployment),
    JournalEntry {
        entry_index: EntryIndex,
        entry: EnrichedRawEntry,
    },
    Suspended {
        waiting_for_completed_entries: HashSet<EntryIndex>,
    },
    // todo remove once we no longer support Bifrost commands written by <= v1.6
    JournalEntryV2 {
        entry: StoredRawEntry,
        /// This is used by the invoker to establish when it's safe to retry
        command_index_to_ack: Option<CommandIndex>,
    },
    JournalEvent {
        event: RawEvent,
    },
    SuspendedV2 {
        /// Flattened set of notification ids derived from `awaiting_on`.
        /// Present only for backward compatibility with Restate < v1.7;
        /// Restate >= v1.7 should use the `awaiting_on` combinator instead.
        /// todo(azmy): drop in version >= v1.8
        waiting_for_notifications: HashSet<journal_v2::NotificationId>,
        /// Combinator tree describing the notifications this invocation is waiting on.
        /// Introduced in Restate v1.7 (protocol version V7). `None` for older invocations.
        /// todo(azmy): make required in >= v1.8
        awaiting_on: Option<NotificationsCombinator>,
    },
    Paused {
        paused_event: RawEvent,
    },
    /// The invoker yielded the invocation back to the scheduler. The partition
    /// processor should re-schedule the invocation (via [`YieldReason`] the
    /// scheduler can apply reason-specific strategies in the future).
    Yield(YieldReason),
    /// This is sent always after [`Self::JournalEntry`] with `OutputStreamEntry`(s).
    End,
    /// This is sent when the invoker exhausted all its attempts to make progress on the specific invocation.
    Failed(InvocationError),
    // New journal entry v2 which only carries the raw entry.
    // Introduced in v1.6.0
    // Start writing in v1.7.0
    JournalEntryV2RawEntry {
        /// This is used by the invoker to establish when it's safe to retry
        command_index_to_ack: Option<CommandIndex>,
        raw_entry: RawEntry,
    },
}

impl EffectKind {
    pub fn journal_entry(
        raw_entry: impl Into<RawEntry>,
        command_index_to_ack: Option<CommandIndex>,
    ) -> Self {
        JournalEntryV2 {
            entry: StoredRawEntry::new(
                StoredRawEntryHeader::new(MillisSinceEpoch::now()),
                raw_entry.into(),
            ),
            command_index_to_ack,
        }
        // todo enable in v1.7.0
        // JournalEntryV2RawEntry {
        //     command_index_to_ack,
        //     raw_entry: raw_entry.into(),
        // }
    }
}

/// Why the invoker yielded the invocation back to the scheduler.
///
/// New reasons can be added without a version barrier — nodes that don't
/// recognize a reason will deserialize it as [`Unknown`](Self::Unknown) and
/// apply the default re-scheduling strategy (immediate re-invoke).
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(tag = "reason"))]
pub enum YieldReason {
    /// The invocation exhausted its outbound memory budget.
    ExhaustedMemoryBudget { needed_memory: NonZeroByteCount },
    /// A yield reason not recognized by this node version. The partition
    /// processor applies the default strategy (re-schedule immediately).
    #[cfg_attr(feature = "serde", serde(other))]
    Unknown,
}

#[derive(Debug, Clone, Copy)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum CombinatorType {
    // Should be treated as FirstCompleted,
    // Used with Suspension V2 to indicate that
    // the sdk did not provide a combinator kind
    Unknown,
    /// Resolve as soon as any one child future completes with success, or with failure (same as JS Promise.race).
    FirstCompleted,
    /// Wait for every child to complete, regardless of success or failure (same as JS Promise.allSettled).
    AllCompleted,
    /// Resolve on the first success; fail only if all children fail (same as JS Promise.any).
    FirstSucceededOrAllFailed,
    /// Resolve when all children succeed; short-circuit on the first failure (same as JS Promise.all).
    AllSucceededOrFirstFailed,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct NotificationsCombinator {
    pub notifications: HashSet<journal_v2::NotificationId>,
    pub nested: Vec<NotificationsCombinator>,
    pub combinator: CombinatorType,
}

impl NotificationsCombinator {
    pub fn is_empty(&self) -> bool {
        self.notifications.is_empty() && self.nested.iter().all(|n| n.is_empty())
    }

    pub fn flatten(&self) -> HashSet<journal_v2::NotificationId> {
        let mut set = HashSet::default();
        self.flatten_inner(&mut set);
        set
    }

    fn flatten_inner(&self, set: &mut HashSet<journal_v2::NotificationId>) {
        for id in &self.notifications {
            set.insert(id.clone());
        }

        for nested in &self.nested {
            nested.flatten_inner(set);
        }
    }
}

impl<T> From<T> for NotificationsCombinator
where
    T: Into<HashSet<journal_v2::NotificationId>>,
{
    fn from(value: T) -> Self {
        let notifications = value.into();
        Self {
            notifications,
            nested: Vec::default(),
            combinator: CombinatorType::Unknown,
        }
    }
}
