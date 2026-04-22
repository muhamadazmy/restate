// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{
    collections::HashSet,
    fmt::{self, Debug},
};

use itertools::{Itertools, Position};
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::journal_v2::{NotificationId, raw::RawNotificationResultVariant};

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CombinatorType {
    // Should be treated as FirstCompleted,
    // Used with Suspension V2 to indicate that
    // the sdk did not provide a combinator kind
    #[default]
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

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub enum UnresolvedFuture {
    Single(NotificationId),
    FistCompleted(Vec<UnresolvedFuture>),
    AllCompleted(Vec<UnresolvedFuture>),
    FirstSucceededOrAllFailed(Vec<UnresolvedFuture>),
    AllSucceededOrFirstFailed(Vec<UnresolvedFuture>),
    Unknown(Vec<UnresolvedFuture>),
}

impl Default for UnresolvedFuture {
    fn default() -> Self {
        Self::Unknown(Vec::default())
    }
}

impl UnresolvedFuture {
    pub fn builder(combinator: CombinatorType) -> UnresolvedFutureBuilder {
        UnresolvedFutureBuilder::new(combinator)
    }

    pub fn unknown_from_iter<I, T>(iter: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<UnresolvedFuture>,
    {
        Self::Unknown(iter.into_iter().map(Into::into).collect())
    }

    pub fn is_empty(&self) -> bool {
        let inner = match self {
            Self::Single(_) => return false,
            Self::Unknown(inner)
            | Self::FistCompleted(inner)
            | Self::AllCompleted(inner)
            | Self::FirstSucceededOrAllFailed(inner)
            | Self::AllSucceededOrFirstFailed(inner) => inner,
        };

        inner.iter().all(|f| f.is_empty())
    }

    pub fn split(
        self,
    ) -> (
        CombinatorType,
        HashSet<NotificationId>,
        Vec<UnresolvedFuture>,
    ) {
        let (combinator, mut inner) = match self {
            Self::Single(notification) => {
                return (
                    CombinatorType::FirstCompleted,
                    HashSet::from([notification]),
                    Vec::default(),
                );
            }
            Self::FistCompleted(inner) => (CombinatorType::FirstCompleted, inner),
            Self::AllCompleted(inner) => (CombinatorType::AllCompleted, inner),
            Self::FirstSucceededOrAllFailed(inner) => {
                (CombinatorType::FirstSucceededOrAllFailed, inner)
            }
            Self::AllSucceededOrFirstFailed(inner) => {
                (CombinatorType::AllSucceededOrFirstFailed, inner)
            }
            Self::Unknown(inner) => {
                let mut notifications = HashSet::new();
                for nested in inner {
                    nested.flatten_inner(&mut notifications);
                }
                return (CombinatorType::Unknown, notifications, Vec::default());
            }
        };

        let mut notifications = HashSet::new();
        let mut i = 0;
        while i < inner.len() {
            if matches!(inner[i], Self::Single(_)) {
                let notification = inner.swap_remove(i);
                match notification {
                    Self::Single(notification) => notifications.insert(notification),
                    _ => unreachable!(),
                };
                continue;
            }

            i += 1;
        }
        (combinator, notifications, inner)
    }

    pub fn flatten(&self) -> HashSet<NotificationId> {
        let mut set = HashSet::default();
        self.flatten_inner(&mut set);
        set
    }

    fn flatten_inner(&self, set: &mut HashSet<NotificationId>) {
        match self {
            Self::Single(notification) => {
                set.insert(notification.clone());
            }
            Self::FistCompleted(futures)
            | Self::AllCompleted(futures)
            | Self::AllSucceededOrFirstFailed(futures)
            | Self::FirstSucceededOrAllFailed(futures)
            | Self::Unknown(futures) => {
                for nested in futures {
                    nested.flatten_inner(set);
                }
            }
        }
    }

    fn resolve_inner(
        &mut self,
        notification_id: &NotificationId,
        result: RawNotificationResultVariant,
    ) -> ResolveResult {
        match self {
            Self::Single(inner) => {
                if inner == notification_id {
                    result.into()
                } else {
                    ResolveResult::Pending
                }
            }
            Self::Unknown(futures) | Self::FistCompleted(futures) => {
                if futures
                    .iter_mut()
                    .any(|f| f.resolve_inner(notification_id, result).is_completed())
                {
                    ResolveResult::Success
                } else {
                    ResolveResult::Pending
                }
            }
            Self::AllCompleted(futures) => {
                futures.retain_mut(|f| f.resolve_inner(notification_id, result).is_pending());

                if futures.is_empty() {
                    ResolveResult::Success
                } else {
                    ResolveResult::Pending
                }
            }
            Self::FirstSucceededOrAllFailed(futures) => {
                let mut i = 0;
                while i < futures.len() {
                    match futures[i].resolve_inner(notification_id, result) {
                        ResolveResult::Success => {
                            return ResolveResult::Success;
                        }
                        ResolveResult::Failure => {
                            futures.swap_remove(i);
                        }
                        ResolveResult::Pending => i += 1,
                    }
                }

                // if all nested futures has been evicted
                // without success, then resolve as failure.
                // otherwise, pending.
                if futures.is_empty() {
                    ResolveResult::Failure
                } else {
                    ResolveResult::Pending
                }
            }
            Self::AllSucceededOrFirstFailed(futures) => {
                let mut i = 0;
                while i < futures.len() {
                    match futures[i].resolve_inner(notification_id, result) {
                        ResolveResult::Success => {
                            futures.swap_remove(i);
                        }
                        ResolveResult::Failure => return ResolveResult::Failure,
                        ResolveResult::Pending => i += 1,
                    }
                }

                // if all nested futures has been evicted
                // without failure, then resolve as success.
                // otherwise, pending.
                if futures.is_empty() {
                    ResolveResult::Success
                } else {
                    ResolveResult::Pending
                }
            }
        }
    }

    /// Applies a single notification to this UnresolvedFuture, advancing its state.
    ///
    /// Returns `true` once the future has reached a terminal state — either
    /// success or failure, as dictated by its [`CombinatorType`]. A `true`
    /// return signals that a suspended invocation waiting on this future can
    /// be resumed; the caller still needs to inspect the remaining state to
    /// determine the outcome.
    ///
    /// Returns `false` if more notifications are needed, or if
    /// `notification_id` did not match anything in this future (including
    /// its nested children).
    pub fn resolve(
        &mut self,
        notification_id: &NotificationId,
        result: RawNotificationResultVariant,
    ) -> bool {
        debug!("Resolving combinator '{self:?}' with ({notification_id}, result: {result:?})");
        if result.is_unknown() {
            // short circuit if the result is unknown. No result variant is associated with
            // this notification. There is no way we can evaluate this future reliably.
            return true;
        }

        self.resolve_inner(notification_id, result).is_completed()
    }

    pub fn resolve_all<'a>(
        &mut self,
        notifications: impl Iterator<Item = (&'a NotificationId, RawNotificationResultVariant)>,
    ) -> bool {
        for (notification_id, result) in notifications {
            if self.resolve(notification_id, result) {
                return true;
            }
        }

        false
    }
}

impl Debug for UnresolvedFuture {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let nested = match self {
            Self::Single(notif) => return write!(f, "{notif}"),
            Self::Unknown(inner) => {
                write!(f, "unknown(")?;
                inner
            }
            Self::FistCompleted(inner) => {
                write!(f, "first_completed(")?;
                inner
            }
            Self::AllCompleted(inner) => {
                write!(f, "all_completed(")?;
                inner
            }
            Self::FirstSucceededOrAllFailed(inner) => {
                write!(f, "first_succeeded_or_all_failed(")?;
                inner
            }
            Self::AllSucceededOrFirstFailed(inner) => {
                write!(f, "all_succeeded_or_first_failed(")?;
                inner
            }
        };

        for (pos, future) in nested.iter().with_position() {
            write!(f, "{future:?}")?;
            if matches!(pos, Position::First | Position::Middle) {
                write!(f, ", ")?;
            }
        }

        write!(f, ")")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ResolveResult {
    Pending,
    Success,
    Failure,
}

impl From<RawNotificationResultVariant> for ResolveResult {
    fn from(value: RawNotificationResultVariant) -> Self {
        if value.is_success() {
            ResolveResult::Success
        } else {
            ResolveResult::Failure
        }
    }
}

impl ResolveResult {
    fn is_pending(&self) -> bool {
        self == &ResolveResult::Pending
    }

    fn is_completed(&self) -> bool {
        matches!(self, Self::Success | Self::Failure)
    }
}

impl From<NotificationId> for UnresolvedFuture {
    fn from(value: NotificationId) -> Self {
        Self::Single(value)
    }
}

pub struct UnresolvedFutureBuilder {
    inner: Vec<UnresolvedFuture>,
    combinator: CombinatorType,
}

impl UnresolvedFutureBuilder {
    pub fn new(combinator: CombinatorType) -> Self {
        Self {
            combinator,
            inner: Vec::default(),
        }
    }

    /// First succeeded or all failed.
    pub fn first_succeeded() -> Self {
        Self {
            inner: Vec::default(),
            combinator: CombinatorType::FirstSucceededOrAllFailed,
        }
    }

    /// All succeeded or first failed.
    pub fn all_succeeded() -> Self {
        Self {
            inner: Vec::default(),
            combinator: CombinatorType::AllSucceededOrFirstFailed,
        }
    }

    /// First completed
    pub fn first_completed() -> Self {
        Self {
            inner: Vec::default(),
            combinator: CombinatorType::FirstCompleted,
        }
    }

    /// All completed
    pub fn all_settled() -> Self {
        Self {
            inner: Vec::default(),
            combinator: CombinatorType::AllCompleted,
        }
    }

    pub fn future(mut self, fut: impl Into<UnresolvedFuture>) -> Self {
        self.inner.push(fut.into());
        self
    }

    pub fn futures<I, T>(mut self, futures: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<UnresolvedFuture>,
    {
        self.inner.extend(futures.into_iter().map(Into::into));
        self
    }

    pub fn build(self) -> UnresolvedFuture {
        match self.combinator {
            CombinatorType::Unknown => UnresolvedFuture::Unknown(self.inner),
            CombinatorType::FirstCompleted => UnresolvedFuture::FistCompleted(self.inner),
            CombinatorType::AllCompleted => UnresolvedFuture::AllCompleted(self.inner),
            CombinatorType::FirstSucceededOrAllFailed => {
                UnresolvedFuture::FirstSucceededOrAllFailed(self.inner)
            }
            CombinatorType::AllSucceededOrFirstFailed => {
                UnresolvedFuture::AllSucceededOrFirstFailed(self.inner)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nid(id: u32) -> NotificationId {
        NotificationId::CompletionId(id)
    }

    const OK: RawNotificationResultVariant = RawNotificationResultVariant::Void;
    const ERR: RawNotificationResultVariant = RawNotificationResultVariant::Failure;

    fn flat_race() -> UnresolvedFuture {
        UnresolvedFutureBuilder::first_completed()
            .future(nid(1))
            .future(nid(2))
            .future(nid(3))
            .build()
    }

    #[test]
    fn race_short_circuits_on_any_completion() {
        let mut fut = flat_race();
        assert!(fut.resolve(&nid(2), OK));

        let mut fut = flat_race();
        assert!(fut.resolve(&nid(1), ERR));

        let mut fut = flat_race();
        assert!(!fut.resolve(&nid(99), OK));
    }

    #[test]
    fn all_settled_requires_every_child() {
        let mut fut = UnresolvedFutureBuilder::all_settled()
            .future(nid(1))
            .future(nid(2))
            .build();

        assert!(!fut.resolve(&nid(1), ERR));
        assert!(fut.resolve(&nid(2), OK));
        assert!(fut.is_empty());
    }

    #[test]
    fn any_first_success_wins_else_all_must_fail() {
        let build = || {
            UnresolvedFutureBuilder::first_succeeded()
                .future(nid(1))
                .future(nid(2))
                .future(nid(3))
                .build()
        };

        let mut fut = build();
        assert!(!fut.resolve(&nid(1), ERR));
        assert!(!fut.resolve(&nid(2), ERR));
        assert!(fut.resolve(&nid(3), ERR));

        let mut fut = build();
        assert!(!fut.resolve(&nid(1), ERR));
        assert!(fut.resolve(&nid(2), OK));
    }

    #[test]
    fn all_first_failure_wins_else_all_must_succeed() {
        let build = || {
            UnresolvedFutureBuilder::all_succeeded()
                .future(nid(1))
                .future(nid(2))
                .future(nid(3))
                .build()
        };

        let mut fut = build();
        assert!(!fut.resolve(&nid(1), OK));
        assert!(fut.resolve(&nid(2), ERR));

        let mut fut = build();
        assert!(!fut.resolve(&nid(1), OK));
        assert!(!fut.resolve(&nid(2), OK));
        assert!(fut.resolve(&nid(3), OK));
    }

    fn leaf_all(a: u32, b: u32) -> UnresolvedFuture {
        UnresolvedFutureBuilder::all_succeeded()
            .future(nid(a))
            .future(nid(b))
            .build()
    }

    fn leaf_race(a: u32, b: u32) -> UnresolvedFuture {
        UnresolvedFutureBuilder::first_completed()
            .future(nid(a))
            .future(nid(b))
            .build()
    }

    fn leaf_any(a: u32, b: u32) -> UnresolvedFuture {
        UnresolvedFutureBuilder::first_succeeded()
            .future(nid(a))
            .future(nid(b))
            .build()
    }

    #[test]
    fn nested_race_of_all_completes_on_first_inner_all() {
        let build = || {
            UnresolvedFutureBuilder::first_completed()
                .future(leaf_all(1, 2))
                .future(leaf_all(3, 4))
                .build()
        };

        let mut fut = build();
        assert!(!fut.resolve(&nid(1), OK));
        assert!(fut.resolve(&nid(2), OK));

        let mut fut = build();
        assert!(fut.resolve(&nid(3), ERR));
    }

    #[test]
    fn nested_all_of_race_completes_when_every_race_completes() {
        let build = || {
            UnresolvedFutureBuilder::all_succeeded()
                .future(leaf_race(1, 2))
                .future(leaf_race(3, 4))
                .build()
        };

        let mut fut = build();
        assert!(!fut.resolve(&nid(1), OK));
        assert!(fut.resolve(&nid(3), OK));

        let mut fut = build();
        assert!(!fut.resolve(&nid(99), OK));
        assert!(!fut.resolve(&nid(1), ERR));
        assert!(fut.resolve(&nid(3), OK));
    }

    #[test]
    fn nested_any_of_all_needs_one_inner_all_to_succeed() {
        let build = || {
            UnresolvedFutureBuilder::first_succeeded()
                .future(leaf_all(1, 2))
                .future(leaf_all(3, 4))
                .build()
        };

        let mut fut = build();
        assert!(!fut.resolve(&nid(1), OK));
        assert!(fut.resolve(&nid(2), OK));

        let mut fut = build();
        assert!(!fut.resolve(&nid(1), ERR));
        assert!(!fut.resolve(&nid(2), ERR));
        assert!(!fut.resolve(&nid(3), OK));
        assert!(fut.resolve(&nid(4), OK));

        let mut fut = build();
        assert!(!fut.resolve(&nid(1), ERR));
        assert!(fut.resolve(&nid(3), ERR));
    }

    #[test]
    fn nested_all_of_any_fails_on_any_inner_failure() {
        let build = || {
            UnresolvedFutureBuilder::all_succeeded()
                .future(leaf_any(1, 2))
                .future(leaf_any(3, 4))
                .build()
        };

        let mut fut = build();
        assert!(!fut.resolve(&nid(1), ERR));
        assert!(fut.resolve(&nid(2), ERR));

        let mut fut = build();
        assert!(!fut.resolve(&nid(1), OK));
        assert!(fut.resolve(&nid(3), OK));
    }

    #[test]
    fn nested_any_of_any_fails_on_all_inner_failure() {
        let build = || {
            UnresolvedFutureBuilder::first_succeeded()
                .future(leaf_any(1, 2))
                .future(leaf_any(3, 4))
                .build()
        };

        let mut fut = build();
        assert!(!fut.resolve(&nid(1), ERR));
        assert!(!fut.resolve(&nid(2), ERR));
        assert!(!fut.resolve(&nid(3), ERR));
        assert!(fut.resolve(&nid(4), ERR));

        let mut fut = build();
        assert!(fut.resolve(&nid(1), OK));
    }

    #[test]
    fn deeply_nested_three_levels() {
        // race(
        //   all(
        //      any(1,2),
        //      any(3,4)
        //   ),
        //   all-settled(5),
        // )

        let build = || {
            let inner_all = UnresolvedFutureBuilder::all_succeeded()
                .future(leaf_any(1, 2))
                .future(leaf_any(3, 4))
                .build();
            let inner_all_settled = UnresolvedFutureBuilder::all_settled()
                .future(nid(5))
                .build();
            UnresolvedFutureBuilder::first_completed()
                .future(inner_all)
                .future(inner_all_settled)
                .build()
        };

        let mut fut = build();
        assert!(!fut.resolve(&nid(1), OK));
        assert!(fut.resolve(&nid(3), OK));

        let mut fut = build();
        assert!(fut.resolve(&nid(5), OK));
    }

    #[test]
    fn nested_combinator_with_mixed_direct_and_nested_children() {
        // all(1, race(2, 3))
        let build = || {
            UnresolvedFutureBuilder::all_succeeded()
                .future(nid(1))
                .future(leaf_race(2, 3))
                .build()
        };

        let mut fut = build();
        assert!(!fut.resolve(&nid(2), OK));
        assert!(fut.resolve(&nid(1), OK));

        let mut fut = build();
        assert!(fut.resolve(&nid(1), ERR));
    }

    #[test]
    fn flatten_and_is_empty_across_nesting() {
        let inner_all = UnresolvedFutureBuilder::all_succeeded()
            .future(nid(3))
            .future(leaf_any(1, 2))
            .build();
        let mut fut = UnresolvedFutureBuilder::first_completed()
            .future(nid(4))
            .future(inner_all)
            .build();

        assert_eq!(
            fut.flatten(),
            HashSet::from([nid(1), nid(2), nid(3), nid(4)])
        );
        assert!(!fut.is_empty());

        // Race resolves via the direct leaf — the nested `all` subtree is
        // not drained, so `is_empty()` still reports false.
        assert!(fut.resolve(&nid(4), OK));
        assert!(!fut.is_empty());
    }

    #[test]
    fn split_special_variants_return_empty_nested_and_flat_notifications() {
        // Single -> FirstCompleted with the single notification, no nested combinators.
        let (combinator, notifications, nested) = UnresolvedFuture::Single(nid(1)).split();
        assert_eq!(combinator, CombinatorType::FirstCompleted);
        assert_eq!(notifications, HashSet::from([nid(1)]));
        assert!(nested.is_empty());

        // Unknown fully flattens every nested subtree into the HashSet.
        let fut = UnresolvedFuture::Unknown(vec![
            UnresolvedFuture::Single(nid(1)),
            leaf_race(2, 3),
            leaf_all(4, 5),
        ]);
        let (combinator, notifications, nested) = fut.split();
        assert_eq!(combinator, CombinatorType::Unknown);
        assert_eq!(
            notifications,
            HashSet::from([nid(1), nid(2), nid(3), nid(4), nid(5)])
        );
        assert!(nested.is_empty());
    }

    #[test]
    fn split_separates_direct_singles_from_nested_combinators() {
        // all(1, race(3, 4), 2, any(5, 6)) -- direct singles are lifted into the HashSet;
        // nested combinators remain in the Vec untouched. Ordering is interleaved so
        // we also exercise the swap_remove path inside the while-loop.
        let fut = UnresolvedFutureBuilder::all_succeeded()
            .future(nid(1))
            .future(leaf_race(3, 4))
            .future(nid(2))
            .future(leaf_any(5, 6))
            .build();
        let (combinator, notifications, nested) = fut.split();
        assert_eq!(combinator, CombinatorType::AllSucceededOrFirstFailed);
        assert_eq!(notifications, HashSet::from([nid(1), nid(2)]));
        assert_eq!(nested.len(), 2);
        let preserved: HashSet<_> = nested.iter().flat_map(|f| f.flatten()).collect();
        assert_eq!(preserved, HashSet::from([nid(3), nid(4), nid(5), nid(6)]));

        // Empty combinator: the type is preserved but nothing is produced.
        let (combinator, notifications, nested) =
            UnresolvedFutureBuilder::all_settled().build().split();
        assert_eq!(combinator, CombinatorType::AllCompleted);
        assert!(notifications.is_empty());
        assert!(nested.is_empty());
    }

    #[test]
    fn resolve_all_iterator_short_circuits_and_returns_true_once_completed() {
        let mut fut = flat_race();
        let batch = [(nid(1), ERR), (nid(2), OK)];
        assert!(fut.resolve_all(batch.iter().map(|(id, r)| (id, *r))));

        let mut fut = flat_race();
        let batch = [(nid(98), OK), (nid(99), OK)];
        assert!(!fut.resolve_all(batch.iter().map(|(id, r)| (id, *r))));
    }
}
