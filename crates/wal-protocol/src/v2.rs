// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::marker::PhantomData;

use bytes::Bytes;

use restate_types::identifiers::{LeaderEpoch, PartitionId, PartitionKey, WithPartitionKey};
use restate_types::logs::{HasRecordKeys, Keys};
use restate_types::storage::{StorageCodecKind, StorageDecode, StorageDecodeError};

pub use records::{
    AnnounceLeader, AttachInvocation, InvocationResponse, Invoke, InvokerEffect,
    NotifyGetInvocationOutputResponse, NotifySignal, PatchState, ProxyThrough, PurgeInvocation,
    PurgeJournal, RestartAsNewInvocation, ResumeInvocation, ScheduleTimer, TerminateInvocation,
    Timer, TruncateInbox, UpdatePartitionDurability, VersionBarrier,
};

pub trait Record: records::Sealed {
    const KIND: RecordKind;
    type Payload: StorageDecode + 'static;
}

/// The primary envelope for all messages in the system.
#[derive(Debug, Clone, bilrost::Message)]
pub struct Envelope<M> {
    #[bilrost(1)]
    pub header: Header,

    #[bilrost(2)]
    record_keys: Keys,

    #[bilrost(3)]
    record: RawRecord,

    phantom: PhantomData<M>,
}

impl<M: Send + Sync> HasRecordKeys for Envelope<M> {
    fn record_keys(&self) -> Keys {
        self.record_keys.clone()
    }
}

impl<M> WithPartitionKey for Envelope<M> {
    fn partition_key(&self) -> PartitionKey {
        match self.header.dest {
            Destination::None => unimplemented!("expect destinationt to be set"),
            Destination::Processor { partition_key, .. } => partition_key,
        }
    }
}

impl<M> Envelope<M> {
    pub fn record_type(&self) -> RecordKind {
        self.record.kind
    }
}

/// Tag for untyped Envelope
pub struct Raw;

impl Envelope<Raw> {
    pub fn into_typed<M: Record>(self) -> Envelope<M> {
        assert_eq!(self.record.kind, M::KIND);
        let Self {
            header,
            record_keys,
            record,
            ..
        } = self;

        Envelope {
            header,
            record_keys,
            record,
            phantom: PhantomData,
        }
    }
}

impl<M: Record> Envelope<M> {
    pub fn payload(&mut self) -> Result<M::Payload, StorageDecodeError> {
        M::Payload::decode(
            &mut self.record.data,
            self.record.encoding.expect("encoding to be set"),
        )
    }

    pub fn into_payload(mut self) -> Result<M::Payload, StorageDecodeError> {
        self.payload()
    }
}

/// Header is set on every message
#[derive(Debug, Clone, bilrost::Message)]
pub struct Header {
    #[bilrost(1)]
    pub source: Source,

    #[bilrost(2)]
    pub dest: Destination,

    #[bilrost(3)]
    pub dedup: Dedup,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, bilrost::Enumeration)]
pub enum RecordKind {
    Unknown = 0,

    AnnounceLeader = 1,
    /// A version barrier to fence off state machine changes that require a certain minimum
    /// version of restate server.
    /// *Since v1.4.0*
    VersionBarrier = 2,
    /// Updates the `PARTITION_DURABILITY` FSM variable to the given value.
    /// See [`PartitionDurability`] for more details.
    ///
    /// *Since v1.4.2*
    UpdatePartitionDurability = 3,

    // -- Partition processor commands
    /// Manual patching of storage state
    PatchState = 4,
    /// Terminate an ongoing invocation
    TerminateInvocation = 5,
    /// Purge a completed invocation
    PurgeInvocation = 6,
    /// Purge a completed invocation journal
    PurgeJournal = 7,
    /// Start an invocation on this partition
    Invoke = 8,
    /// Truncate the message outbox up to, and including, the specified index.
    TruncateOutbox = 9,
    /// Proxy a service invocation through this partition processor, to reuse the deduplication id map.
    ProxyThrough = 10,
    /// Attach to an existing invocation
    AttachInvocation = 11,
    /// Resume an invocation
    ResumeInvocation = 12,
    /// Restart as new invocation from prefix
    RestartAsNewInvocation = 13,
    // -- Partition processor events for PP
    /// Invoker is reporting effect(s) from an ongoing invocation.
    InvokerEffect = 14,
    /// Timer has fired
    Timer = 15,
    /// Schedule timer
    ScheduleTimer = 16,
    /// Another partition processor is reporting a response of an invocation we requested.
    ///
    /// KINDA DEPRECATED: When Journal Table V1 is removed, this command should be used only to reply to invocations.
    /// Now it's abused for a bunch of other scenarios, like replying to get promise and get invocation output.
    ///
    /// For more details see `OnNotifyInvocationResponse`.
    InvocationResponse = 17,

    // -- New PP <-> PP commands using Journal V2
    /// Notify Get invocation output
    NotifyGetInvocationOutputResponse = 18,
    /// Notify a signal.
    NotifySignal = 19,
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct RawRecord {
    #[bilrost(1)]
    kind: RecordKind,
    #[bilrost(2)]
    encoding: Option<StorageCodecKind>,
    #[bilrost(3)]
    data: Bytes,
}

/// Identifies the source of a message
#[derive(Debug, Clone, bilrost::Oneof, bilrost::Message)]
pub enum Source {
    #[bilrost(empty)]
    None,

    /// Message is sent from an ingress node
    #[bilrost(tag = 1, message)]
    Ingress,

    /// Message is sent from some control plane component (controller, cli, etc.)
    #[bilrost(tag = 2, message)]
    ControlPlane,

    /// Message is sent from another partition processor
    #[bilrost(tag = 3, message)]
    Processor {
        /// if possible, this is used to reroute responses in case of splits/merges
        /// Marked as `Option` in v1.5. Note that v1.4 requires this to be set but as of v1.6
        /// this can be safely set to `None`.
        #[bilrost(1)]
        partition_id: Option<PartitionId>,
        #[bilrost(2)]
        partition_key: Option<PartitionKey>,
        /// The current epoch of the partition leader. Readers should observe this to decide which
        /// messages to accept. Readers should ignore messages coming from
        /// epochs lower than the max observed for a given partition id.
        #[bilrost(3)]
        leader_epoch: LeaderEpoch,
    },
}

/// Identifies the intended destination of the message
#[derive(Debug, Clone, bilrost::Oneof, bilrost::Message)]
pub enum Destination {
    #[bilrost(empty)]
    None,

    /// Message is sent to partition processor
    #[bilrost(tag = 1, message)]
    Processor {
        partition_key: PartitionKey,
        // dedup: Option<DedupInformation>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Default, bilrost::Oneof, bilrost::Message)]
pub enum Dedup {
    #[default]
    None,
    /// Sequence number to deduplicate messages sent by the same partition or a successor
    /// of a previous partition (a successor partition will inherit the leader epoch of its
    /// predecessor).
    #[bilrost(tag(1), message)]
    SelfProposal {
        #[bilrost(0)]
        leader_epoch: LeaderEpoch,
        #[bilrost(1)]
        seq: u64,
    },
    /// Sequence number to deduplicate messages from a foreign partition.
    #[bilrost(tag(2), message)]
    ForeignPartition {
        #[bilrost(0)]
        partition: PartitionId,
        #[bilrost(1)]
        seq: u64,
    },
    /// Sequence number to deduplicate messages from an arbitrary string prefix.
    #[bilrost(tag(3), message)]
    Arbitrary {
        #[bilrost(0)]
        prefix: String,
        #[bilrost(1)]
        seq: u64,
    },
}

mod records {

    use restate_types::{
        invocation::{
            AttachInvocationRequest, GetInvocationOutputResponse, InvocationTermination,
            NotifySignalRequest, PurgeInvocationRequest, RestartAsNewInvocationRequest,
            ResumeInvocationRequest, ServiceInvocation,
        },
        message::MessageIndexRecrod,
        state_mut::ExternalStateMutation,
    };

    use crate::timer::TimerKeyValue;

    use super::{Record, RecordKind};

    pub trait Sealed {}

    macro_rules! record {
        {@name=$name:ident, @kind=$type:expr, @payload=$payload:path} => {
            #[allow(dead_code)]
            pub struct $name;
            impl Sealed for $name{}
            impl Record for $name {
                const KIND: RecordKind = $type;
                type Payload = $payload;
            }
        };
    }

    record! {
        @name=AnnounceLeader,
        @kind=RecordKind::AnnounceLeader,
        @payload=crate::control::AnnounceLeader
    }

    record! {
        @name=VersionBarrier,
        @kind=RecordKind::VersionBarrier,
        @payload=crate::control::VersionBarrier
    }

    record! {
        @name=UpdatePartitionDurability,
        @kind=RecordKind::UpdatePartitionDurability,
        @payload=crate::control::PartitionDurability
    }

    record! {
        @name=PatchState,
        @kind=RecordKind::PatchState,
        @payload=ExternalStateMutation
    }

    record! {
        @name=TerminateInvocation,
        @kind=RecordKind::TerminateInvocation,
        @payload=InvocationTermination
    }

    record! {
        @name=PurgeInvocation,
        @kind=RecordKind::PurgeInvocation,
        @payload=PurgeInvocationRequest
    }

    record! {
        @name=PurgeJournal,
        @kind=RecordKind::PurgeJournal,
        @payload=PurgeInvocationRequest
    }

    record! {
        @name=Invoke,
        @kind=RecordKind::Invoke,
        @payload=ServiceInvocation
    }

    record! {
        @name=TruncateInbox,
        @kind=RecordKind::TruncateOutbox,
        @payload=MessageIndexRecrod
    }

    record! {
        @name=ProxyThrough,
        @kind=RecordKind::ProxyThrough,
        @payload=ServiceInvocation
    }

    record! {
        @name=AttachInvocation,
        @kind=RecordKind::AttachInvocation,
        @payload=AttachInvocationRequest
    }

    record! {
        @name=ResumeInvocation,
        @kind=RecordKind::ResumeInvocation,
        @payload=ResumeInvocationRequest
    }

    record! {
        @name=RestartAsNewInvocation,
        @kind=RecordKind::RestartAsNewInvocation,
        @payload=RestartAsNewInvocationRequest
    }

    record! {
        @name=InvokerEffect,
        @kind=RecordKind::InvokerEffect,
        @payload=restate_invoker_api::Effect
    }

    record! {
        @name=Timer,
        @kind=RecordKind::Timer,
        @payload=TimerKeyValue
    }

    record! {
        @name=ScheduleTimer,
        @kind=RecordKind::ScheduleTimer,
        @payload=TimerKeyValue
    }

    record! {
        @name=InvocationResponse,
        @kind=RecordKind::InvocationResponse,
        @payload=restate_types::invocation::InvocationResponse
    }

    record! {
        @name=NotifyGetInvocationOutputResponse,
        @kind=RecordKind::NotifyGetInvocationOutputResponse,
        @payload=GetInvocationOutputResponse
    }

    record! {
        @name=NotifySignal,
        @kind=RecordKind::NotifySignal,
        @payload=NotifySignalRequest
    }
}
