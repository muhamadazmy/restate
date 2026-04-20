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
use std::fmt::{self, Debug};

use bytes::Bytes;
use enum_dispatch::enum_dispatch;
use itertools::{Itertools, Position};
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::identifiers::InvocationId;
use crate::journal_v2::raw::{
    RawEntry, RawNotificationResultVariant, TryFromEntry, TryFromEntryError,
};
use crate::journal_v2::{
    CompletionId, Encoder, Entry, EntryMetadata, EntryType, Failure, SignalIndex, SignalName,
};

/// See [`Notification`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NotificationId {
    CompletionId(CompletionId),
    SignalIndex(SignalIndex),
    SignalName(SignalName),
}

impl NotificationId {
    pub const fn for_completion(id: CompletionId) -> Self {
        Self::CompletionId(id)
    }

    pub fn for_signal(signal_id: SignalId) -> Self {
        match signal_id {
            SignalId::Index(idx) => NotificationId::SignalIndex(idx),
            SignalId::Name(n) => NotificationId::SignalName(n),
        }
    }
}

impl From<SignalId> for NotificationId {
    fn from(value: SignalId) -> Self {
        NotificationId::for_signal(value)
    }
}

impl fmt::Display for NotificationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NotificationId::SignalIndex(idx) => write!(f, "SignalIndex: {idx}"),
            NotificationId::SignalName(name) => write!(f, "SignalName: {name}"),
            NotificationId::CompletionId(idx) => write!(f, "CompletionId: {idx}"),
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NotificationType {
    Completion(CompletionType),
    Signal,
}

impl fmt::Display for NotificationType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NotificationType::Completion(cmp) => fmt::Display::fmt(cmp, f),
            e => fmt::Debug::fmt(e, f),
        }
    }
}

impl From<CompletionType> for NotificationType {
    fn from(value: CompletionType) -> Self {
        NotificationType::Completion(value)
    }
}

impl From<CompletionType> for EntryType {
    fn from(value: CompletionType) -> Self {
        EntryType::Notification(value.into())
    }
}

#[enum_dispatch]
pub trait NotificationMetadata {
    fn id(&self) -> NotificationId;
}

#[enum_dispatch(NotificationMetadata, EntryMetadata)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Notification {
    Completion(Completion),
    Signal(Signal),
}

impl Notification {
    pub fn new_completion(completion: impl Into<Completion>) -> Self {
        Self::Completion(completion.into())
    }

    pub fn new_signal(signal: Signal) -> Self {
        Self::Signal(signal)
    }

    pub fn encode<E: Encoder>(self) -> RawEntry {
        E::encode_entry(Entry::Notification(self))
    }
}

#[enum_dispatch(NotificationMetadata, EntryMetadata)]
#[derive(Debug, Clone, PartialEq, Eq, strum::EnumDiscriminants, Serialize, Deserialize)]
#[strum_discriminants(vis(pub))]
#[strum_discriminants(name(CompletionType))]
#[strum_discriminants(derive(
    serde::Serialize,
    serde::Deserialize,
    strum::EnumString,
    strum::IntoStaticStr
))]
pub enum Completion {
    GetLazyState(GetLazyStateCompletion),
    GetLazyStateKeys(GetLazyStateKeysCompletion),
    GetPromise(GetPromiseCompletion),
    PeekPromise(PeekPromiseCompletion),
    CompletePromise(CompletePromiseCompletion),
    Sleep(SleepCompletion),
    CallInvocationId(CallInvocationIdCompletion),
    Call(CallCompletion),
    Run(RunCompletion),
    AttachInvocation(AttachInvocationCompletion),
    GetInvocationOutput(GetInvocationOutputCompletion),
}

impl fmt::Display for CompletionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl From<Completion> for Entry {
    fn from(value: Completion) -> Self {
        Entry::Notification(Notification::Completion(value))
    }
}

// Little macro to reduce boilerplate for TryFromEntry and EntryMetadata.
macro_rules! impl_completion_accessors {
    ($ty:ident -> []) => {
        // End of macro
    };
    ($ty:ident -> [@from_entry $($tail:tt)*]) => {
        impl TryFromEntry for paste::paste! { [< $ty Completion >] } {
            fn try_from(entry: Entry) -> Result<Self, TryFromEntryError> {
                match entry {
                    Entry::Notification(Notification::Completion(Completion::$ty(e))) => Ok(e),
                    e => Err(TryFromEntryError {
                        expected: EntryType::Notification(NotificationType::Completion(CompletionType::$ty)),
                        actual: e.ty(),
                    }),
                }
            }
        }
        impl From<paste::paste! { [< $ty Completion >] }> for Entry {
            fn from(v: paste::paste! { [< $ty Completion >] }) -> Self {
                Self::Notification(v.into())
            }
        }
        impl From<paste::paste! { [< $ty Completion >] }> for Notification {
            fn from(v: paste::paste! { [< $ty Completion >] }) -> Self {
                Self::Completion(v.into())
            }
        }
        impl_completion_accessors!($ty -> [$($tail)*]);
    };
    ($ty:ident -> [@entry_metadata $($tail:tt)*]) => {
        impl EntryMetadata for paste::paste! { [< $ty Completion >] } {
            fn ty(&self) -> EntryType {
                EntryType::Notification(NotificationType::Completion(CompletionType::$ty))
            }
        }
        impl_completion_accessors!($ty -> [$($tail)*]);
    };
    ($ty:ident -> [@notification_metadata $($tail:tt)*]) => {
        impl NotificationMetadata for paste::paste! { [< $ty Completion >] } {
            fn id(&self) -> NotificationId {
                NotificationId::CompletionId(self.completion_id)
            }
        }
        impl_completion_accessors!($ty -> [$($tail)*]);
    };

    // Entrypoints of the macro
    ($ty:ident: [$($tokens:tt)*]) => {
        impl_completion_accessors!($ty -> [$($tokens)*]);
    };
    ($ty:ident) => {
        impl_completion_accessors!($ty -> [@entry_metadata @notification_metadata @from_entry]);
    };
}

// --- Actual implementation of individual notifications

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetLazyStateCompletion {
    pub completion_id: CompletionId,
    pub result: GetStateResult,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GetStateResult {
    Void,
    Success(Bytes),
}
impl_completion_accessors!(GetLazyState);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetLazyStateKeysCompletion {
    pub completion_id: CompletionId,
    pub state_keys: Vec<String>,
}
impl_completion_accessors!(GetLazyStateKeys);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetPromiseCompletion {
    pub completion_id: CompletionId,
    pub result: GetPromiseResult,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GetPromiseResult {
    Success(Bytes),
    Failure(Failure),
}
impl_completion_accessors!(GetPromise);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeekPromiseCompletion {
    pub completion_id: CompletionId,
    pub result: PeekPromiseResult,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PeekPromiseResult {
    Void,
    Success(Bytes),
    Failure(Failure),
}
impl_completion_accessors!(PeekPromise);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletePromiseCompletion {
    pub completion_id: CompletionId,
    pub result: CompletePromiseResult,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompletePromiseResult {
    Void,
    Failure(Failure),
}
impl_completion_accessors!(CompletePromise);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SleepCompletion {
    pub completion_id: CompletionId,
}
impl_completion_accessors!(Sleep);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallInvocationIdCompletion {
    pub completion_id: CompletionId,
    pub invocation_id: InvocationId,
}
impl_completion_accessors!(CallInvocationId);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallCompletion {
    pub completion_id: CompletionId,
    pub result: CallResult,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CallResult {
    Success(Bytes),
    Failure(Failure),
}
impl_completion_accessors!(Call);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunCompletion {
    pub completion_id: CompletionId,
    pub result: RunResult,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunResult {
    Success(Bytes),
    Failure(Failure),
}
impl_completion_accessors!(Run);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttachInvocationCompletion {
    pub completion_id: CompletionId,
    pub result: AttachInvocationResult,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AttachInvocationResult {
    Success(Bytes),
    Failure(Failure),
}
impl_completion_accessors!(AttachInvocation);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetInvocationOutputCompletion {
    pub completion_id: CompletionId,
    pub result: GetInvocationOutputResult,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, bilrost::Oneof)]
pub enum GetInvocationOutputResult {
    Void,
    #[bilrost(2)]
    Success(Bytes),
    #[bilrost(3)]
    Failure(Failure),
}
impl_completion_accessors!(GetInvocationOutput);

// Signal

#[repr(u32)]
#[derive(Debug, strum::FromRepr)]
pub enum BuiltInSignal {
    Cancel = 1,
}

pub const CANCEL_SIGNAL: Notification = Notification::Signal(Signal::new(
    SignalId::for_builtin_signal(BuiltInSignal::Cancel),
    SignalResult::Void,
));

pub const CANCEL_NOTIFICATION_ID: NotificationId =
    NotificationId::SignalIndex(BuiltInSignal::Cancel as u32);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SignalId {
    Index(SignalIndex),
    Name(SignalName),
}

impl fmt::Display for SignalId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SignalId::Index(idx) => {
                if let Some(built_in_signal) = BuiltInSignal::from_repr(*idx) {
                    write!(f, "{built_in_signal:?}")
                } else {
                    write!(f, "index {idx}")
                }
            }
            SignalId::Name(name) => write!(f, "{name}"),
        }
    }
}

impl SignalId {
    pub const fn for_builtin_signal(signal: BuiltInSignal) -> Self {
        Self::for_index(signal as u32)
    }

    pub const fn for_index(id: SignalIndex) -> Self {
        Self::Index(id)
    }

    pub fn for_name(id: SignalName) -> Self {
        Self::Name(id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Signal {
    pub id: SignalId,
    pub result: SignalResult,
}

impl Signal {
    pub const fn new(id: SignalId, result: SignalResult) -> Self {
        Self { id, result }
    }
}

impl EntryMetadata for Signal {
    fn ty(&self) -> EntryType {
        EntryType::Notification(NotificationType::Signal)
    }
}

impl NotificationMetadata for Signal {
    fn id(&self) -> NotificationId {
        self.id.clone().into()
    }
}

impl TryFromEntry for Signal {
    fn try_from(entry: Entry) -> Result<Self, TryFromEntryError> {
        match entry {
            Entry::Notification(Notification::Signal(e)) => Ok(e),
            e => Err(TryFromEntryError {
                expected: EntryType::Notification(NotificationType::Signal),
                actual: e.ty(),
            }),
        }
    }
}

impl From<Signal> for Entry {
    fn from(v: Signal) -> Self {
        Self::Notification(v.into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SignalResult {
    Void,
    Success(Bytes),
    Failure(Failure),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct UnresolvedFuture {
    pub notifications: HashSet<NotificationId>,
    pub nested: Vec<UnresolvedFuture>,
    pub combinator: CombinatorType,
}

impl UnresolvedFuture {
    pub fn is_empty(&self) -> bool {
        self.notifications.is_empty() && self.nested.iter().all(|n| n.is_empty())
    }

    pub fn flatten(&self) -> HashSet<NotificationId> {
        let mut set = HashSet::default();
        self.flatten_inner(&mut set);
        set
    }

    fn flatten_inner(&self, set: &mut HashSet<NotificationId>) {
        for id in &self.notifications {
            set.insert(id.clone());
        }

        for nested in &self.nested {
            nested.flatten_inner(set);
        }
    }

    fn resolve_first_completed(
        &mut self,
        notification_id: &NotificationId,
        result: RawNotificationResultVariant,
    ) -> ResolveResult {
        if self.notifications.remove(notification_id) {
            return ResolveResult::Success;
        }

        if self
            .nested
            .iter_mut()
            .any(|v| v.resolve_inner(notification_id, result).is_completed())
        {
            ResolveResult::Success
        } else {
            ResolveResult::Pending
        }
    }

    fn resolve_all_completed(
        &mut self,
        notification_id: &NotificationId,
        result: RawNotificationResultVariant,
    ) -> ResolveResult {
        // All child notifications must be completed (regardless of the the result)
        self.notifications.remove(notification_id);
        self.nested
            .retain_mut(|n| n.resolve_inner(notification_id, result).is_pending());

        if self.notifications.is_empty() && self.nested.is_empty() {
            ResolveResult::Success
        } else {
            ResolveResult::Pending
        }
    }

    fn resolve_first_succeeded_or_all_failed(
        &mut self,
        notification_id: &NotificationId,
        result: RawNotificationResultVariant,
    ) -> ResolveResult {
        if self.notifications.remove(notification_id) && result.is_success() {
            return ResolveResult::Success;
        }

        let mut i = 0;
        while i < self.nested.len() {
            match self.nested[i].resolve_inner(notification_id, result) {
                ResolveResult::Success => {
                    return ResolveResult::Success;
                }
                ResolveResult::Failure => {
                    // swap_remove brings the tail element to position i, so
                    // keep i put and re-examine it on the next iteration.
                    self.nested.swap_remove(i);
                }
                ResolveResult::Pending => i += 1,
            }
        }

        // if all notifications and nested combinator has been evicted
        // without success, then resolve as failure.
        // otherwise, pending.
        if self.notifications.is_empty() && self.nested.is_empty() {
            ResolveResult::Failure
        } else {
            ResolveResult::Pending
        }
    }

    fn resolve_all_succeed_or_first_failed(
        &mut self,
        notification_id: &NotificationId,
        result: RawNotificationResultVariant,
    ) -> ResolveResult {
        if self.notifications.remove(notification_id) && !result.is_success() {
            return ResolveResult::Failure;
        }

        let mut i = 0;
        while i < self.nested.len() {
            match self.nested[i].resolve_inner(notification_id, result) {
                ResolveResult::Success => {
                    // swap_remove brings the tail element to position i, so
                    // keep i put and re-examine it on the next iteration.
                    self.nested.swap_remove(i);
                }
                ResolveResult::Failure => return ResolveResult::Failure,
                ResolveResult::Pending => i += 1,
            }
        }

        // if all notifications and nested combinator has been evicted
        // without failure, then resolve as success.
        // otherwise, pending.
        if self.notifications.is_empty() && self.nested.is_empty() {
            ResolveResult::Success
        } else {
            ResolveResult::Pending
        }
    }

    fn resolve_inner(
        &mut self,
        notification_id: &NotificationId,
        result: RawNotificationResultVariant,
    ) -> ResolveResult {
        match self.combinator {
            CombinatorType::Unknown | CombinatorType::FirstCompleted => {
                self.resolve_first_completed(notification_id, result)
            }
            CombinatorType::AllCompleted => self.resolve_all_completed(notification_id, result),
            CombinatorType::FirstSucceededOrAllFailed => {
                self.resolve_first_succeeded_or_all_failed(notification_id, result)
            }
            CombinatorType::AllSucceededOrFirstFailed => {
                self.resolve_all_succeed_or_first_failed(notification_id, result)
            }
        }
    }

    /// Applies a single notification to this combinator, advancing its state.
    ///
    /// Returns `true` once the combinator has reached a terminal state — either
    /// success or failure, as dictated by its [`CombinatorType`]. A `true`
    /// return signals that a suspended invocation waiting on this future can
    /// be resumed; the caller still needs to inspect the remaining state to
    /// determine the outcome.
    ///
    /// Returns `false` if more notifications are needed, or if
    /// `notification_id` did not match anything in this combinator (including
    /// its nested children).
    pub fn resolve(
        &mut self,
        notification_id: &NotificationId,
        result: RawNotificationResultVariant,
    ) -> bool {
        debug!("Resolving combinator '{self:?}' with ({notification_id}, result: {result:?})");
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

#[derive(Debug, Clone, PartialEq, Eq)]
enum ResolveResult {
    Pending,
    Success,
    Failure,
}

impl ResolveResult {
    fn is_pending(&self) -> bool {
        self == &ResolveResult::Pending
    }

    fn is_completed(&self) -> bool {
        matches!(self, Self::Success | Self::Failure)
    }
}

impl<T> From<T> for UnresolvedFuture
where
    T: Into<HashSet<NotificationId>>,
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

impl Debug for UnresolvedFuture {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.combinator {
            CombinatorType::Unknown => write!(f, "unknown"),
            CombinatorType::FirstCompleted => write!(f, "race"),
            CombinatorType::AllCompleted => write!(f, "all-settled"),
            CombinatorType::FirstSucceededOrAllFailed => write!(f, "any"),
            CombinatorType::AllSucceededOrFirstFailed => write!(f, "all"),
        }?;

        write!(f, "(")?;
        for (pos, notification_id) in self.notifications.iter().with_position() {
            write!(f, "{notification_id}")?;
            if matches!(pos, Position::First | Position::Middle) {
                write!(f, ", ")?;
            }
        }

        for (pos, nested) in self.nested.iter().with_position() {
            write!(f, "{nested:?}")?;
            if matches!(pos, Position::First | Position::Middle) {
                write!(f, ", ")?;
            }
        }

        write!(f, ")")
    }
}

pub struct UnresolvedFutureBuilder {
    inner: UnresolvedFuture,
}

impl UnresolvedFutureBuilder {
    /// First succeeded or all failed.
    pub fn any() -> Self {
        Self {
            inner: UnresolvedFuture {
                notifications: HashSet::default(),
                nested: Vec::default(),
                combinator: CombinatorType::FirstSucceededOrAllFailed,
            },
        }
    }

    /// All succeeded or first failed.
    pub fn all() -> Self {
        Self {
            inner: UnresolvedFuture {
                notifications: HashSet::default(),
                nested: Vec::default(),
                combinator: CombinatorType::AllSucceededOrFirstFailed,
            },
        }
    }

    /// First completed
    pub fn race() -> Self {
        Self {
            inner: UnresolvedFuture {
                notifications: HashSet::default(),
                nested: Vec::default(),
                combinator: CombinatorType::FirstCompleted,
            },
        }
    }

    pub fn all_settled() -> Self {
        Self {
            inner: UnresolvedFuture {
                notifications: HashSet::default(),
                nested: Vec::default(),
                combinator: CombinatorType::AllCompleted,
            },
        }
    }

    pub fn notification(mut self, notification_id: NotificationId) -> Self {
        self.inner.notifications.insert(notification_id);
        self
    }

    pub fn combinator(mut self, combinator: UnresolvedFuture) -> Self {
        self.inner.nested.push(combinator);
        self
    }

    pub fn build(self) -> UnresolvedFuture {
        self.inner
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

    #[test]
    fn builder_constructs_correct_combinator_type() {
        assert_eq!(
            UnresolvedFutureBuilder::any().build().combinator,
            CombinatorType::FirstSucceededOrAllFailed
        );
        assert_eq!(
            UnresolvedFutureBuilder::all().build().combinator,
            CombinatorType::AllSucceededOrFirstFailed
        );
        assert_eq!(
            UnresolvedFutureBuilder::race().build().combinator,
            CombinatorType::FirstCompleted
        );
        assert_eq!(
            UnresolvedFutureBuilder::all_settled().build().combinator,
            CombinatorType::AllCompleted
        );

        let fut = UnresolvedFutureBuilder::race()
            .notification(nid(1))
            .notification(nid(2))
            .combinator(UnresolvedFutureBuilder::all().notification(nid(3)).build())
            .build();
        assert_eq!(fut.notifications, HashSet::from([nid(1), nid(2)]));
        assert_eq!(fut.nested.len(), 1);
        assert_eq!(
            fut.nested[0].combinator,
            CombinatorType::AllSucceededOrFirstFailed
        );
        assert_eq!(fut.nested[0].notifications, HashSet::from([nid(3)]));
    }

    fn flat_race() -> UnresolvedFuture {
        UnresolvedFutureBuilder::race()
            .notification(nid(1))
            .notification(nid(2))
            .notification(nid(3))
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
            .notification(nid(1))
            .notification(nid(2))
            .build();

        assert!(!fut.resolve(&nid(1), ERR));
        assert!(fut.resolve(&nid(2), OK));
        assert!(fut.is_empty());
    }

    #[test]
    fn any_first_success_wins_else_all_must_fail() {
        let build = || {
            UnresolvedFutureBuilder::any()
                .notification(nid(1))
                .notification(nid(2))
                .notification(nid(3))
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
            UnresolvedFutureBuilder::all()
                .notification(nid(1))
                .notification(nid(2))
                .notification(nid(3))
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
        UnresolvedFutureBuilder::all()
            .notification(nid(a))
            .notification(nid(b))
            .build()
    }

    fn leaf_race(a: u32, b: u32) -> UnresolvedFuture {
        UnresolvedFutureBuilder::race()
            .notification(nid(a))
            .notification(nid(b))
            .build()
    }

    fn leaf_any(a: u32, b: u32) -> UnresolvedFuture {
        UnresolvedFutureBuilder::any()
            .notification(nid(a))
            .notification(nid(b))
            .build()
    }

    #[test]
    fn nested_race_of_all_completes_on_first_inner_all() {
        let build = || {
            UnresolvedFutureBuilder::race()
                .combinator(leaf_all(1, 2))
                .combinator(leaf_all(3, 4))
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
            UnresolvedFutureBuilder::all()
                .combinator(leaf_race(1, 2))
                .combinator(leaf_race(3, 4))
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
            UnresolvedFutureBuilder::any()
                .combinator(leaf_all(1, 2))
                .combinator(leaf_all(3, 4))
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
            UnresolvedFutureBuilder::all()
                .combinator(leaf_any(1, 2))
                .combinator(leaf_any(3, 4))
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
            UnresolvedFutureBuilder::any()
                .combinator(leaf_any(1, 2))
                .combinator(leaf_any(3, 4))
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
            let inner_all = UnresolvedFutureBuilder::all()
                .combinator(leaf_any(1, 2))
                .combinator(leaf_any(3, 4))
                .build();
            let inner_all_settled = UnresolvedFutureBuilder::all_settled()
                .notification(nid(5))
                .build();
            UnresolvedFutureBuilder::race()
                .combinator(inner_all)
                .combinator(inner_all_settled)
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
            UnresolvedFutureBuilder::all()
                .notification(nid(1))
                .combinator(leaf_race(2, 3))
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
        let inner_all = UnresolvedFutureBuilder::all()
            .notification(nid(3))
            .combinator(leaf_any(1, 2))
            .build();
        let mut fut = UnresolvedFutureBuilder::race()
            .notification(nid(4))
            .combinator(inner_all)
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
    fn resolve_all_iterator_short_circuits_and_returns_true_once_completed() {
        let mut fut = flat_race();
        let batch = [(nid(1), ERR), (nid(2), OK)];
        assert!(fut.resolve_all(batch.iter().map(|(id, r)| (id, *r))));

        let mut fut = flat_race();
        let batch = [(nid(98), OK), (nid(99), OK)];
        assert!(!fut.resolve_all(batch.iter().map(|(id, r)| (id, *r))));
    }
}
