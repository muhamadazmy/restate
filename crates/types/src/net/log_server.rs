// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::Infallible;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use bitflags::bitflags;
use prost_dto::{FromProst, IntoProst};
use serde::{Deserialize, Serialize};

use super::codec::{V2Convertible, WireDecode, WireEncode};
use super::{RpcRequest, TargetName};

use crate::GenerationalNodeId;
use crate::errors::ConversionError;
use crate::logs::{KeyFilter, LogletId, LogletOffset, Record, SequenceNumber, TailState};
use crate::time::MillisSinceEpoch;

pub trait LogServerRequest: RpcRequest + WireEncode + Sync + Send + 'static {
    fn header(&self) -> &LogServerRequestHeader;
    fn header_mut(&mut self) -> &mut LogServerRequestHeader;
    fn refresh_header(&mut self, known_global_tail: LogletOffset) {
        let loglet_id = self.header().loglet_id;
        *self.header_mut() = LogServerRequestHeader {
            loglet_id,
            known_global_tail,
        }
    }
}

pub trait LogServerResponse: WireDecode + Sync + Send {
    fn header(&self) -> &LogServerResponseHeader;
}

macro_rules! define_logserver_rpc {
    (
        @request = $request:ty,
        @response = $response:ty,
        @request_target = $request_target:expr,
        @response_target = $response_target:expr,
    ) => {
        crate::net::define_rpc! {
            @request = $request,
            @response = $response,
            @request_target = $request_target,
            @response_target = $response_target,
        }

        impl LogServerRequest for $request {
            fn header(&self) -> &LogServerRequestHeader {
                &self.header
            }

            fn header_mut(&mut self) -> &mut LogServerRequestHeader {
                &mut self.header
            }
        }

        impl LogServerResponse for $response {
            fn header(&self) -> &LogServerResponseHeader {
                &self.header
            }
        }
    };
    (
        @proto = ProtocolVersion::Bilrost,
        @request = $request:ty,
        @response = $response:ty,
        @request_target = $request_target:expr,
        @response_target = $response_target:expr,
    ) => {
        crate::net::define_rpc! {
            @proto = ProtocolVersion::Bilrost,
            @request = $request,
            @response = $response,
            @request_target = $request_target,
            @response_target = $response_target,
        }

        impl LogServerRequest for $request {
            fn header(&self) -> &LogServerRequestHeader {
                &self.header
            }

            fn header_mut(&mut self) -> &mut LogServerRequestHeader {
                &mut self.header
            }
        }

        impl LogServerResponse for $response {
            fn header(&self) -> &LogServerResponseHeader {
                &self.header
            }
        }
    };
}

#[derive(
    Debug,
    Clone,
    Copy,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    IntoProst,
    FromProst,
    bilrost::Enumeration,
)]
#[prost(target = "crate::protobuf::log_server_common::Status")]
#[repr(u8)]
pub enum Status {
    /// Operation was successful
    Ok = 1,
    /// The node's storage system is disabled and cannot accept operations at the moment.
    Disabled = 2,
    /// If the operation expired or not completed due to load shedding. The operation can be
    /// retried by the client. It's guaranteed that this store has not been persisted by the node.
    Dropped = 3,
    /// Operation rejected on a sealed loglet
    Sealed = 4,
    /// Loglet is being sealed and operation cannot be accepted
    Sealing = 5,
    /// Operation has been rejected. Operation requires that the sender is the authoritative
    /// sequencer.
    SequencerMismatch = 6,
    /// This indicates that the operation cannot be accepted due to the offset being out of bounds.
    /// For instance, if a store is sent to a log-server that with a lagging local commit offset.
    OutOfBounds = 7,
    /// The record is malformed, this could be because it has too many records or any other reason
    /// that leads the server to reject processing it.
    Malformed = 8,
}

// ----- LogServer API -----
// Requests: Bifrost -> LogServer //
// Responses LogServer -> Bifrost //

// Store
define_logserver_rpc! {
    @proto = ProtocolVersion::Bilrost,
    @request = Store,
    @response = Stored,
    @request_target = TargetName::LogServerStore,
    @response_target = TargetName::LogServerStored,
}

// Release
define_logserver_rpc! {
    @proto = ProtocolVersion::Bilrost,
    @request = Release,
    @response = Released,
    @request_target = TargetName::LogServerRelease,
    @response_target = TargetName::LogServerReleased,
}

// Seal
define_logserver_rpc! {
    @proto = ProtocolVersion::Bilrost,
    @request = Seal,
    @response = Sealed,
    @request_target = TargetName::LogServerSeal,
    @response_target = TargetName::LogServerSealed,
}

// GetLogletInfo
define_logserver_rpc! {
    @proto = ProtocolVersion::Bilrost,
    @request = GetLogletInfo,
    @response = LogletInfo,
    @request_target = TargetName::LogServerGetLogletInfo,
    @response_target = TargetName::LogServerLogletInfo,
}

// Trim
define_logserver_rpc! {
    @proto = ProtocolVersion::Bilrost,
    @request = Trim,
    @response = Trimmed,
    @request_target = TargetName::LogServerTrim,
    @response_target = TargetName::LogServerTrimmed,
}

// GetRecords
define_logserver_rpc! {
    @proto = ProtocolVersion::Bilrost,
    @request = GetRecords,
    @response = Records,
    @request_target = TargetName::LogServerGetRecords,
    @response_target = TargetName::LogServerRecords,
}

// WaitForTail
define_logserver_rpc! {
    @proto = ProtocolVersion::Bilrost,
    @request = WaitForTail,
    @response = TailUpdated,
    @request_target = TargetName::LogServerWaitForTail,
    @response_target = TargetName::LogServerTailUpdated,
}

// GetDigest
define_logserver_rpc! {
    @proto = ProtocolVersion::Bilrost,
    @request = GetDigest,
    @response = Digest,
    @request_target = TargetName::LogServerGetDigest,
    @response_target = TargetName::LogServerDigest,
}

#[derive(Debug, Clone, Serialize, Deserialize, bilrost::Message)]
pub struct LogServerRequestHeader {
    #[bilrost(tag(1))]
    pub loglet_id: LogletId,
    /// If the sender has now knowledge of this value, it can safely be set to
    /// `LogletOffset::INVALID`
    #[bilrost(tag(2))]
    pub known_global_tail: LogletOffset,
}

impl LogServerRequestHeader {
    pub fn new(loglet_id: LogletId, known_global_tail: LogletOffset) -> Self {
        Self {
            loglet_id,
            known_global_tail,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, IntoProst, FromProst)]
#[prost(target = "crate::protobuf::log_server_common::ResponseHeader")]
pub struct LogServerResponseHeader {
    /// The position after the last locally committed record on this node
    pub local_tail: LogletOffset,
    /// The node's view of the last global tail of the loglet. If unknown, it
    /// can be safely set to `LogletOffset::INVALID`.
    pub known_global_tail: LogletOffset,
    /// Whether this node has sealed or not (local to the log-server)
    pub sealed: bool,
    pub status: Status,
}

impl LogServerResponseHeader {
    pub fn new(local_tail_state: TailState<LogletOffset>, known_global_tail: LogletOffset) -> Self {
        Self {
            local_tail: local_tail_state.offset(),
            known_global_tail,
            sealed: local_tail_state.is_sealed(),
            status: Status::Ok,
        }
    }

    pub fn empty() -> Self {
        Self {
            local_tail: LogletOffset::INVALID,
            known_global_tail: LogletOffset::INVALID,
            sealed: false,
            status: Status::Disabled,
        }
    }
}

// ** STORE
#[derive(Debug, Clone, Serialize, Deserialize, bilrost::Message)]
pub struct StoreFlags(u32);
bitflags! {
    impl StoreFlags: u32 {
        const IgnoreSeal = 0b000_00001;
    }
}

/// Store one or more records on a log-server
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Store {
    pub header: LogServerRequestHeader,
    // The receiver should skip handling this message if it hasn't started to act on it
    // before timeout expires.
    pub timeout_at: Option<MillisSinceEpoch>,
    pub flags: StoreFlags,
    /// Offset of the first record in the batch of payloads. Payloads in the batch get a gap-less
    /// range of offsets that starts with (includes) the value of `first_offset`.
    pub first_offset: LogletOffset,
    /// This is the sequencer identifier for this log. This should be set even for repair store messages.
    pub sequencer: GenerationalNodeId,
    /// Denotes the last record that has been safely uploaded to an archiving data store.
    pub known_archived: LogletOffset,
    // todo (asoli) serialize efficiently
    pub payloads: Arc<[Record]>,
}

impl Store {
    /// The message's timeout has passed, we should discard if possible.
    pub fn expired(&self) -> bool {
        self.timeout_at
            .is_some_and(|timeout_at| MillisSinceEpoch::now() >= timeout_at)
    }

    // returns None on overflow
    pub fn last_offset(&self) -> Option<LogletOffset> {
        let len: u32 = self.payloads.len().try_into().ok()?;
        self.first_offset.checked_add(len - 1).map(Into::into)
    }

    pub fn estimated_encode_size(&self) -> usize {
        self.payloads
            .iter()
            .map(|p| p.estimated_encode_size())
            .sum()
    }
}

impl V2Convertible for Store {
    type Target = message::Store;
    type Error = Infallible;

    fn from_v2(value: Self::Target) -> Result<Self, Self::Error> {
        Ok(value.into())
    }

    fn into_v2(self) -> Self::Target {
        self.into()
    }
}

/// Response to a `Store` request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stored {
    pub header: LogServerResponseHeader,
}

impl V2Convertible for Stored {
    // Since Stored message is just the header, we can implement
    // Convertible directly to the LogServerResponseHeader type
    type Target = message::LogServerResponseHeader;
    type Error = ConversionError;

    fn from_v2(value: Self::Target) -> Result<Self, Self::Error> {
        Ok(Stored {
            header: value.try_into()?,
        })
    }

    fn into_v2(self) -> Self::Target {
        self.header.into()
    }
}

impl Deref for Stored {
    type Target = LogServerResponseHeader;

    fn deref(&self) -> &Self::Target {
        &self.header
    }
}

impl DerefMut for Stored {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.header
    }
}

impl Stored {
    pub fn empty() -> Self {
        Self {
            header: LogServerResponseHeader::empty(),
        }
    }

    pub fn new(tail_state: TailState<LogletOffset>, known_global_tail: LogletOffset) -> Self {
        Self {
            header: LogServerResponseHeader::new(tail_state, known_global_tail),
        }
    }

    pub fn with_status(mut self, status: Status) -> Self {
        self.header.status = status;
        self
    }
}

// ** RELEASE
#[derive(Debug, Clone, Serialize, Deserialize, bilrost::Message)]
pub struct Release {
    #[bilrost(1)]
    pub header: LogServerRequestHeader,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Released {
    pub header: LogServerResponseHeader,
}

impl Released {
    pub fn empty() -> Self {
        Self {
            header: LogServerResponseHeader::empty(),
        }
    }

    pub fn new(tail_state: TailState<LogletOffset>, known_global_tail: LogletOffset) -> Self {
        Self {
            header: LogServerResponseHeader::new(tail_state, known_global_tail),
        }
    }

    pub fn status(mut self, status: Status) -> Self {
        self.header.status = status;
        self
    }
}

impl V2Convertible for Released {
    type Target = message::LogServerResponseHeader;
    type Error = ConversionError;

    fn from_v2(value: Self::Target) -> Result<Self, Self::Error> {
        Ok(Self {
            header: value.try_into()?,
        })
    }

    fn into_v2(self) -> Self::Target {
        self.header.into()
    }
}

// ** SEAL
/// Seals the loglet so no further stores can be accepted
#[derive(Debug, Clone, Serialize, Deserialize, bilrost::Message)]
pub struct Seal {
    #[bilrost(1)]
    pub header: LogServerRequestHeader,
    /// This is the sequencer identifier for this log. This should be set even for repair store messages.
    #[bilrost(2)]
    pub sequencer: GenerationalNodeId,
}

/// Response to a `Seal` request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sealed {
    pub header: LogServerResponseHeader,
}

impl V2Convertible for Sealed {
    type Target = message::LogServerResponseHeader;
    type Error = ConversionError;

    fn from_v2(value: Self::Target) -> Result<Self, Self::Error> {
        Ok(Self {
            header: value.try_into()?,
        })
    }

    fn into_v2(self) -> Self::Target {
        self.header.into()
    }
}

impl Deref for Sealed {
    type Target = LogServerResponseHeader;

    fn deref(&self) -> &Self::Target {
        &self.header
    }
}

impl DerefMut for Sealed {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.header
    }
}

impl Sealed {
    pub fn empty() -> Self {
        Self {
            header: LogServerResponseHeader::empty(),
        }
    }

    pub fn new(tail_state: TailState<LogletOffset>, known_global_tail: LogletOffset) -> Self {
        Self {
            header: LogServerResponseHeader::new(tail_state, known_global_tail),
        }
    }

    pub fn with_status(mut self, status: Status) -> Self {
        self.header.status = status;
        self
    }
}

// ** GET_LOGLET_INFO
#[derive(Debug, Clone, Serialize, Deserialize, bilrost::Message)]
pub struct GetLogletInfo {
    #[bilrost(1)]
    pub header: LogServerRequestHeader,
}

#[derive(Debug, Clone, Serialize, Deserialize, IntoProst)]
#[prost(target = "crate::protobuf::log_server_common::LogletInfo")]
pub struct LogletInfo {
    #[prost(required)]
    pub header: LogServerResponseHeader,
    pub trim_point: LogletOffset,
}

impl Deref for LogletInfo {
    type Target = LogServerResponseHeader;

    fn deref(&self) -> &Self::Target {
        &self.header
    }
}

impl DerefMut for LogletInfo {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.header
    }
}

impl LogletInfo {
    pub fn empty() -> Self {
        Self {
            header: LogServerResponseHeader::empty(),
            trim_point: LogletOffset::INVALID,
        }
    }

    pub fn new(
        tail_state: TailState<LogletOffset>,
        trim_point: LogletOffset,
        known_global_tail: LogletOffset,
    ) -> Self {
        Self {
            header: LogServerResponseHeader::new(tail_state, known_global_tail),
            trim_point,
        }
    }

    pub fn with_status(mut self, status: Status) -> Self {
        self.header.status = status;
        self
    }
}

impl V2Convertible for LogletInfo {
    type Target = message::LogletInfo;
    type Error = ConversionError;

    fn from_v2(value: Self::Target) -> Result<Self, Self::Error> {
        value.try_into()
    }

    fn into_v2(self) -> Self::Target {
        self.into()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, bilrost::Message)]
pub struct Gap {
    /// to is inclusive
    #[bilrost(1)]
    pub to: LogletOffset,
}

#[derive(Debug, Clone, derive_more::IsVariant, derive_more::TryUnwrap, Serialize, Deserialize)]
pub enum MaybeRecord {
    TrimGap(Gap),
    ArchivalGap(Gap),
    // Record(s) existed but got filtered out
    FilteredGap(Gap),
    Data(Record),
}

// ** GET_RECORDS

/// Returns a batch that includes **all** records that the node has between
/// `from_offset` and `to_offset` that match the filter. This might return different results if
/// more records were replicated behind the known_global_commit/local_tail after the original
/// request.
///
/// That said, it must not return "fewer" records unless there was a trim or archival of old
/// records.
///
/// If `to_offset` is higher than `local_tail`, then we return all records up-to the `local_tail`
/// and the value of `local_tail` in the response header will indicate what is the snapshot of the
/// local tail that was used during the read process and `next_offset` will be set accordingly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetRecords {
    pub header: LogServerRequestHeader,
    /// if set, the server will stop reading when the next record will tip of the total number of
    /// bytes allocated. The returned `next_offset` can be used by the reader to move the cursor
    /// for subsequent reads.
    ///
    /// Note the limit is not strict, but it's a practical mechanism to limit the client memory
    /// buffer when reading from multiple servers. Additionally. It'll always try to get a single
    /// record even if that record exceeds the stated budget.
    pub total_limit_in_bytes: Option<usize>,
    /// Only read records that match the filter.
    pub filter: KeyFilter,
    /// inclusive
    pub from_offset: LogletOffset,
    /// inclusive (will be clipped to local_tail-1), actual value of local tail is set on the
    /// response header.
    pub to_offset: LogletOffset,
}

impl V2Convertible for GetRecords {
    type Target = message::GetRecords;
    type Error = Infallible;

    fn from_v2(value: Self::Target) -> Result<Self, Self::Error> {
        Ok(value.into())
    }

    fn into_v2(self) -> Self::Target {
        self.into()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Records {
    pub header: LogServerResponseHeader,
    /// Indicates the next offset to read from after this response. This is useful when
    /// the response is partial due to hitting budgeting limits (memory, buffer, etc.)
    ///
    /// If the returned set of records include all records originally requested, the
    /// value of next_offset will be set to `to_offset + 1`. On errors this will be set to the
    /// value of `from_offset`.
    pub next_offset: LogletOffset,
    /// Sorted by offset
    pub records: Vec<(LogletOffset, MaybeRecord)>,
}

impl Deref for Records {
    type Target = LogServerResponseHeader;

    fn deref(&self) -> &Self::Target {
        &self.header
    }
}

impl DerefMut for Records {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.header
    }
}

impl Records {
    pub fn empty(next_offset: LogletOffset) -> Self {
        Self {
            header: LogServerResponseHeader::empty(),
            records: Vec::default(),
            next_offset,
        }
    }

    pub fn new(
        tail_state: TailState<LogletOffset>,
        known_global_tail: LogletOffset,
        next_offset: LogletOffset,
    ) -> Self {
        Self {
            header: LogServerResponseHeader::new(tail_state, known_global_tail),
            records: Vec::default(),
            next_offset,
        }
    }

    pub fn with_status(mut self, status: Status) -> Self {
        self.header.status = status;
        self
    }
}

impl V2Convertible for Records {
    type Target = message::Records;
    type Error = ConversionError;

    fn from_v2(value: Self::Target) -> Result<Self, Self::Error> {
        value.try_into()
    }

    fn into_v2(self) -> Self::Target {
        self.into()
    }
}

// ** TRIM
#[derive(Debug, Clone, Serialize, Deserialize, bilrost::Message)]
pub struct Trim {
    pub header: LogServerRequestHeader,
    /// The trim_point is inclusive (will be trimmed)
    pub trim_point: LogletOffset,
}

/// Response to a `Trim` request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trimmed {
    pub header: LogServerResponseHeader,
}

impl V2Convertible for Trimmed {
    type Target = message::LogServerResponseHeader;
    type Error = ConversionError;

    fn from_v2(value: Self::Target) -> Result<Self, Self::Error> {
        Ok(Self {
            header: value.try_into()?,
        })
    }

    fn into_v2(self) -> Self::Target {
        self.header.into()
    }
}

impl Deref for Trimmed {
    type Target = LogServerResponseHeader;

    fn deref(&self) -> &Self::Target {
        &self.header
    }
}

impl DerefMut for Trimmed {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.header
    }
}

impl Trimmed {
    pub fn empty() -> Self {
        Self {
            header: LogServerResponseHeader::empty(),
        }
    }

    pub fn new(tail_state: TailState<LogletOffset>, known_global_tail: LogletOffset) -> Self {
        Self {
            header: LogServerResponseHeader::new(tail_state, known_global_tail),
        }
    }

    pub fn with_status(mut self, status: Status) -> Self {
        self.header.status = status;
        self
    }
}

// ** WAIT_FOR_TAIL

/// Defines the tail we are interested in waiting for.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TailUpdateQuery {
    /// The node's local tail must be at or higher than this value
    LocalTail(LogletOffset),
    /// The node must observe the global tail at or higher than this value
    GlobalTail(LogletOffset),
    /// Either the local tail or the global tail arriving at this value will resolve this request.
    LocalOrGlobal(LogletOffset),
}

/// Subscribes to a notification that will be sent when the log-server reaches a minimum local-tail
/// or global-tail value OR if the node is sealed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WaitForTail {
    pub header: LogServerRequestHeader,
    /// If the caller is not interested in observing a specific tail value (i.e. only interested in
    /// the seal signal), this should be set to `TailUpdateQuery::GlobalTail(LogletOffset::MAX)`.
    pub query: TailUpdateQuery,
}

impl V2Convertible for WaitForTail {
    type Target = message::WaitForTail;
    type Error = ConversionError;

    fn from_v2(value: Self::Target) -> Result<Self, Self::Error> {
        value.try_into()
    }

    fn into_v2(self) -> Self::Target {
        self.into()
    }
}

/// Response to a `WaitForTail` request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TailUpdated {
    pub header: LogServerResponseHeader,
}

impl V2Convertible for TailUpdated {
    type Target = message::LogServerResponseHeader;
    type Error = ConversionError;

    fn from_v2(value: Self::Target) -> Result<Self, Self::Error> {
        Ok(Self {
            header: value.try_into()?,
        })
    }

    fn into_v2(self) -> Self::Target {
        self.header.into()
    }
}

impl Deref for TailUpdated {
    type Target = LogServerResponseHeader;

    fn deref(&self) -> &Self::Target {
        &self.header
    }
}

impl DerefMut for TailUpdated {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.header
    }
}

impl TailUpdated {
    pub fn empty() -> Self {
        Self {
            header: LogServerResponseHeader::empty(),
        }
    }

    pub fn new(tail_state: TailState<LogletOffset>, known_global_tail: LogletOffset) -> Self {
        Self {
            header: LogServerResponseHeader::new(tail_state, known_global_tail),
        }
    }

    pub fn with_status(mut self, status: Status) -> Self {
        self.header.status = status;
        self
    }
}

// ** GET_DIGEST

/// Request a digest of the loglet between two offsets from this node
#[derive(Debug, Clone, Serialize, Deserialize, bilrost::Message)]
pub struct GetDigest {
    pub header: LogServerRequestHeader,
    // inclusive
    pub from_offset: LogletOffset,
    pub to_offset: LogletOffset,
}

#[derive(
    Debug, Clone, PartialEq, Eq, derive_more::Display, Serialize, Deserialize, IntoProst, FromProst,
)]
#[prost(target = "crate::protobuf::log_server_common::DigestEntry")]
#[display("[{from_offset}..{to_offset}] -> {status} ({})",  self.len())]
pub struct DigestEntry {
    // inclusive
    pub from_offset: LogletOffset,
    pub to_offset: LogletOffset,
    pub status: RecordStatus,
}

#[derive(
    Debug,
    Clone,
    Eq,
    PartialEq,
    derive_more::Display,
    Serialize,
    Deserialize,
    IntoProst,
    FromProst,
    bilrost::Enumeration,
)]
#[repr(u8)]
#[prost(target = crate::protobuf::log_server_common::RecordStatus)]
pub enum RecordStatus {
    #[display("T")]
    Trimmed = 1,
    #[display("A")]
    Archived = 2,
    #[display("X")]
    Exists = 3,
}

impl DigestEntry {
    // how many offsets are included in this entry
    pub fn len(&self) -> usize {
        if self.to_offset >= self.from_offset {
            return 0;
        }

        usize::try_from(self.to_offset.saturating_sub(*self.from_offset)).expect("no overflow") + 1
    }

    pub fn is_empty(&self) -> bool {
        self.from_offset > self.to_offset
    }
}

/// Response to a `GetDigest` request
#[derive(Debug, Clone, Serialize, Deserialize, IntoProst, FromProst)]
#[prost(target = "crate::protobuf::log_server_common::Digest")]
pub struct Digest {
    #[prost(required)]
    pub header: LogServerResponseHeader,
    // If the node's local trim-point (or archival-point) overlaps with the digest range, an entry will be
    // added to include where the trim-gap ends. Otherwise, offsets for non-existing records
    // will not be included in the response.
    //
    // Entries are sorted by `from_offset`. The response header includes the node's local tail and
    // its known_global_tail as per usual.
    //
    // entries's contents must be ignored if `status` != `Status::Ok`.
    pub entries: Vec<DigestEntry>,
}

impl V2Convertible for Digest {
    type Target = message::Digest;
    type Error = ConversionError;

    fn from_v2(value: Self::Target) -> Result<Self, Self::Error> {
        value.try_into()
    }

    fn into_v2(self) -> Self::Target {
        self.into()
    }
}

impl Digest {
    pub fn empty() -> Self {
        Self {
            header: LogServerResponseHeader::empty(),
            entries: Default::default(),
        }
    }

    pub fn new(
        tail_state: TailState<LogletOffset>,
        known_global_tail: LogletOffset,
        entries: Vec<DigestEntry>,
    ) -> Self {
        Self {
            header: LogServerResponseHeader::new(tail_state, known_global_tail),
            entries,
        }
    }

    pub fn with_status(mut self, status: Status) -> Self {
        self.header.status = status;
        self
    }
}

mod message {

    use std::sync::Arc;

    use crate::{
        GenerationalNodeId,
        errors::ConversionError,
        logs::LogletOffset,
        storage::PolyBytes,
        time::{MillisSinceEpoch, NanosSinceEpoch},
    };

    use super::{Gap, LogServerRequestHeader, RecordStatus, Status, StoreFlags};

    #[derive(Debug, Clone, bilrost::Message)]
    pub struct LogServerResponseHeader {
        /// The position after the last locally committed record on this node
        #[bilrost(1)]
        local_tail: LogletOffset,
        /// The node's view of the last global tail of the loglet. If unknown, it
        /// can be safely set to `LogletOffset::INVALID`.
        #[bilrost(2)]
        known_global_tail: LogletOffset,
        /// Whether this node has sealed or not (local to the log-server)
        #[bilrost(3)]
        sealed: bool,
        #[bilrost(4)]
        status: Option<Status>,
    }

    impl From<super::LogServerResponseHeader> for LogServerResponseHeader {
        fn from(value: super::LogServerResponseHeader) -> Self {
            let super::LogServerResponseHeader {
                local_tail,
                known_global_tail,
                sealed,
                status,
            } = value;

            Self {
                local_tail,
                known_global_tail,
                sealed,
                status: Some(status),
            }
        }
    }

    impl TryFrom<LogServerResponseHeader> for super::LogServerResponseHeader {
        type Error = ConversionError;

        fn try_from(value: LogServerResponseHeader) -> Result<Self, Self::Error> {
            let LogServerResponseHeader {
                known_global_tail,
                local_tail,
                sealed,
                status,
            } = value;

            Ok(Self {
                known_global_tail,
                local_tail,
                sealed,
                status: status.ok_or_else(|| ConversionError::missing_field("status"))?,
            })
        }
    }

    #[derive(Debug, Clone, Default, bilrost::Oneof)]
    /// The keys that are associated with a record. This is used to filter the log when reading.
    pub enum Keys {
        /// No keys are associated with the record. This record will appear to *all* readers regardless
        /// of the KeyFilter they use.
        #[default]
        None,
        /// A single key is associated with the record
        #[bilrost(3)]
        Single(u64),
        /// A pair of keys are associated with the record
        #[bilrost(4)]
        Pair((u64, u64)),
        /// The record is associated with all keys within this range (inclusive)
        #[bilrost(5)]
        RangeInclusive((u64, u64)),
    }

    impl From<crate::logs::Keys> for Keys {
        fn from(value: crate::logs::Keys) -> Self {
            match value {
                crate::logs::Keys::None => Self::None,
                crate::logs::Keys::Single(key) => Self::Single(key),
                crate::logs::Keys::Pair(k1, k2) => Self::Pair((k1, k2)),
                crate::logs::Keys::RangeInclusive(range) => {
                    Self::RangeInclusive((*range.start(), *range.end()))
                }
            }
        }
    }

    impl From<Keys> for crate::logs::Keys {
        fn from(value: Keys) -> Self {
            match value {
                Keys::None => crate::logs::Keys::None,
                Keys::Single(key) => crate::logs::Keys::Single(key),
                Keys::Pair((k1, k2)) => crate::logs::Keys::Pair(k1, k2),
                Keys::RangeInclusive((start, end)) => Self::RangeInclusive(start..=end),
            }
        }
    }

    #[derive(Debug, Clone, bilrost::Message)]
    pub struct Record {
        #[bilrost(1)]
        created_at: NanosSinceEpoch,
        #[bilrost(2)]
        body: bytes::Bytes,
        #[bilrost(oneof(3, 4, 5))]
        keys: Keys,
    }

    impl From<crate::logs::Record> for Record {
        fn from(value: crate::logs::Record) -> Self {
            let (created_at, keys, body) = value.into_parts();

            Self {
                created_at,
                body: match body {
                    PolyBytes::Bytes(buf) => buf,
                    PolyBytes::Typed(obj) => {
                        // todo: create buf with enough capacity
                        let mut buf = bytes::BytesMut::new();
                        obj.encode(&mut buf).expect("should be serializable");
                        buf.into()
                    }
                },
                keys: keys.into(),
            }
        }
    }

    impl From<Record> for crate::logs::Record {
        fn from(value: Record) -> Self {
            let Record {
                created_at,
                body,
                keys,
            } = value;

            Self::from_parts(created_at, keys.into(), PolyBytes::Bytes(body))
        }
    }

    /// Store one or more records on a log-server
    #[derive(Debug, Clone, bilrost::Message)]
    pub struct Store {
        #[bilrost(1)]
        header: LogServerRequestHeader,
        // The receiver should skip handling this message if it hasn't started to act on it
        // before timeout expires.
        #[bilrost(2)]
        timeout_at: Option<MillisSinceEpoch>,
        #[bilrost(3)]
        flags: StoreFlags,
        /// Offset of the first record in the batch of payloads. Payloads in the batch get a gap-less
        /// range of offsets that starts with (includes) the value of `first_offset`.
        #[bilrost(4)]
        first_offset: LogletOffset,
        /// This is the sequencer identifier for this log. This should be set even for repair store messages.
        #[bilrost(5)]
        sequencer: GenerationalNodeId,
        /// Denotes the last record that has been safely uploaded to an archiving data store.
        #[bilrost(6)]
        known_archived: LogletOffset,
        #[bilrost(7)]
        payloads: Vec<Record>,
    }

    impl From<super::Store> for Store {
        fn from(value: super::Store) -> Self {
            let super::Store {
                header,
                timeout_at,
                flags,
                first_offset,
                sequencer,
                known_archived,
                payloads,
            } = value;

            Self {
                header,
                timeout_at,
                flags,
                first_offset,
                sequencer,
                known_archived,
                payloads: {
                    // todo: can we use payloads.iter().cloned().map(Into::into).collect()
                    // or it's better to create the vec ahead with capacity! will collect
                    // respect length hint?

                    let mut values = Vec::with_capacity(payloads.len());
                    for record in payloads.iter() {
                        values.push(record.clone().into());
                    }
                    values
                },
            }
        }
    }

    impl From<Store> for super::Store {
        fn from(value: Store) -> Self {
            let Store {
                header,
                timeout_at,
                flags,
                first_offset,
                sequencer,
                known_archived,
                payloads,
            } = value;

            Self {
                header,
                timeout_at,
                flags,
                first_offset,
                sequencer,
                known_archived,
                payloads: {
                    // todo: can we use payloads.iter().cloned().map(Into::into).collect()
                    // or it's better to create the vec ahead with capacity! will collect
                    // respect length hint?

                    let mut values = Vec::with_capacity(payloads.len());
                    for record in payloads.iter() {
                        values.push(record.clone().into());
                    }

                    Arc::from(values)
                },
            }
        }
    }

    #[derive(Debug, Clone, bilrost::Message)]
    pub struct LogletInfo {
        #[bilrost(1)]
        header: LogServerResponseHeader,
        #[bilrost(2)]
        trim_point: LogletOffset,
    }

    impl From<super::LogletInfo> for LogletInfo {
        fn from(value: super::LogletInfo) -> Self {
            let super::LogletInfo { header, trim_point } = value;

            Self {
                header: header.into(),
                trim_point,
            }
        }
    }

    impl TryFrom<LogletInfo> for super::LogletInfo {
        type Error = ConversionError;
        fn try_from(value: LogletInfo) -> Result<Self, Self::Error> {
            let LogletInfo { header, trim_point } = value;

            Ok(Self {
                header: header.try_into()?,
                trim_point,
            })
        }
    }

    #[derive(Clone, Copy, Debug, bilrost::Oneof, Default)]
    enum KeyFilter {
        #[default]
        // Matches any record
        Any,

        // Match records that have a specific key, or no keys at all.
        #[bilrost(3)]
        Include(u64),
        // Match records that have _any_ keys falling within this inclusive range,
        // in addition to records with no keys.
        #[bilrost(4)]
        Within((u64, u64)),
    }

    impl From<crate::logs::KeyFilter> for KeyFilter {
        fn from(value: crate::logs::KeyFilter) -> Self {
            use crate::logs::KeyFilter::*;
            match value {
                Any => Self::Any,
                Include(key) => Self::Include(key),
                Within(range) => Self::Within((*range.start(), *range.end())),
            }
        }
    }

    impl From<KeyFilter> for crate::logs::KeyFilter {
        fn from(value: KeyFilter) -> Self {
            match value {
                KeyFilter::Any => Self::Any,
                KeyFilter::Include(key) => Self::Include(key),
                KeyFilter::Within((start, end)) => Self::Within(start..=end),
            }
        }
    }

    #[derive(Debug, Clone, bilrost::Message)]
    pub struct GetRecords {
        #[bilrost(1)]
        header: LogServerRequestHeader,
        #[bilrost(2)]
        total_limit_in_bytes: Option<usize>,
        #[bilrost(oneof(3, 4))]
        filter: KeyFilter,
        #[bilrost(5)]
        from_offset: LogletOffset,
        #[bilrost(6)]
        to_offset: LogletOffset,
    }

    impl From<super::GetRecords> for GetRecords {
        fn from(value: super::GetRecords) -> Self {
            let super::GetRecords {
                header,
                total_limit_in_bytes,
                filter,
                from_offset,
                to_offset,
            } = value;

            Self {
                header,
                total_limit_in_bytes,
                filter: filter.into(),
                from_offset,
                to_offset,
            }
        }
    }

    impl From<GetRecords> for super::GetRecords {
        fn from(value: GetRecords) -> Self {
            let GetRecords {
                header,
                total_limit_in_bytes,
                filter,
                from_offset,
                to_offset,
            } = value;

            super::GetRecords {
                header,
                total_limit_in_bytes,
                filter: filter.into(),
                from_offset,
                to_offset,
            }
        }
    }

    #[derive(Debug, Clone, bilrost::Oneof)]
    enum MaybeGap {
        Data,
        #[bilrost(1)]
        TrimGap(Gap),
        #[bilrost(2)]
        ArchivalGap(Gap),
        #[bilrost(3)]
        FilteredGap(Gap),
    }

    #[derive(Debug, Clone, bilrost::Message)]
    struct MaybeRecord {
        #[bilrost(oneof(1, 2, 3))]
        gap: MaybeGap,
        // record is only set of gap == Data
        #[bilrost(4)]
        record: Option<Record>,
    }

    impl From<super::MaybeRecord> for MaybeRecord {
        fn from(value: super::MaybeRecord) -> Self {
            let (gap, record) = match value {
                super::MaybeRecord::TrimGap(gap) => (MaybeGap::TrimGap(gap), None),
                super::MaybeRecord::ArchivalGap(gap) => (MaybeGap::ArchivalGap(gap), None),
                super::MaybeRecord::FilteredGap(gap) => (MaybeGap::FilteredGap(gap), None),
                super::MaybeRecord::Data(record) => (MaybeGap::Data, Some(record)),
            };

            Self {
                gap,
                record: record.map(Into::into),
            }
        }
    }

    impl TryFrom<MaybeRecord> for super::MaybeRecord {
        type Error = ConversionError;

        fn try_from(value: MaybeRecord) -> Result<Self, Self::Error> {
            let MaybeRecord { gap, record } = value;

            let result = match gap {
                MaybeGap::Data => Self::Data(
                    record
                        .ok_or_else(|| ConversionError::missing_field("record"))?
                        .into(),
                ),
                MaybeGap::TrimGap(gap) => Self::TrimGap(gap),
                MaybeGap::ArchivalGap(gap) => Self::ArchivalGap(gap),
                MaybeGap::FilteredGap(gap) => Self::FilteredGap(gap),
            };

            Ok(result)
        }
    }

    #[derive(Debug, Clone, bilrost::Message)]
    pub struct Records {
        #[bilrost(1)]
        header: LogServerResponseHeader,
        #[bilrost(2)]
        next_offset: LogletOffset,
        #[bilrost(3)]
        records: Vec<(LogletOffset, MaybeRecord)>,
    }

    impl From<super::Records> for Records {
        fn from(value: super::Records) -> Self {
            let super::Records {
                header,
                next_offset,
                records,
            } = value;

            Self {
                header: header.into(),
                next_offset,
                records: records
                    .into_iter()
                    .map(|(offset, maybe_record)| (offset, maybe_record.into()))
                    .collect(),
            }
        }
    }

    impl TryFrom<Records> for super::Records {
        type Error = ConversionError;

        fn try_from(value: Records) -> Result<Self, Self::Error> {
            let Records {
                header,
                next_offset,
                records,
            } = value;

            Ok(Self {
                header: header.try_into()?,
                next_offset,
                records: {
                    let mut result = Vec::with_capacity(records.len());
                    for (offset, maybe_record) in records {
                        result.push((offset, maybe_record.try_into()?));
                    }
                    result
                },
            })
        }
    }

    /// Defines the tail we are interested in waiting for.
    #[derive(Debug, Clone, bilrost::Oneof)]
    enum TailUpdateQuery {
        /// The node's local tail must be at or higher than this value
        #[bilrost(2)]
        LocalTail(LogletOffset),
        /// The node must observe the global tail at or higher than this value
        #[bilrost(3)]
        GlobalTail(LogletOffset),
        /// Either the local tail or the global tail arriving at this value will resolve this request.
        #[bilrost(4)]
        LocalOrGlobal(LogletOffset),
    }

    impl From<super::TailUpdateQuery> for TailUpdateQuery {
        fn from(value: super::TailUpdateQuery) -> Self {
            use super::TailUpdateQuery::*;

            match value {
                LocalTail(offset) => Self::LocalTail(offset),
                GlobalTail(offset) => Self::GlobalTail(offset),
                LocalOrGlobal(offset) => Self::LocalOrGlobal(offset),
            }
        }
    }
    impl From<TailUpdateQuery> for super::TailUpdateQuery {
        fn from(value: TailUpdateQuery) -> Self {
            match value {
                TailUpdateQuery::LocalTail(offset) => Self::LocalTail(offset),
                TailUpdateQuery::GlobalTail(offset) => Self::GlobalTail(offset),
                TailUpdateQuery::LocalOrGlobal(offset) => Self::LocalOrGlobal(offset),
            }
        }
    }

    /// Subscribes to a notification that will be sent when the log-server reaches a minimum local-tail
    /// or global-tail value OR if the node is sealed.
    #[derive(Debug, Clone, bilrost::Message)]
    pub struct WaitForTail {
        #[bilrost(1)]
        header: LogServerRequestHeader,
        /// If the caller is not interested in observing a specific tail value (i.e. only interested in
        /// the seal signal), this should be set to `TailUpdateQuery::GlobalTail(LogletOffset::MAX)`.
        #[bilrost(oneof(2, 3, 4))]
        query: Option<TailUpdateQuery>,
    }

    impl From<super::WaitForTail> for WaitForTail {
        fn from(value: super::WaitForTail) -> Self {
            let super::WaitForTail { header, query } = value;
            Self {
                header,
                query: Some(query.into()),
            }
        }
    }

    impl TryFrom<WaitForTail> for super::WaitForTail {
        type Error = ConversionError;

        fn try_from(value: WaitForTail) -> Result<Self, Self::Error> {
            let WaitForTail { header, query } = value;

            Ok(Self {
                header,
                query: query
                    .ok_or_else(|| ConversionError::missing_field("query"))?
                    .into(),
            })
        }
    }

    #[derive(Debug, Clone, bilrost::Message)]
    pub struct DigestEntry {
        // inclusive
        from_offset: LogletOffset,
        to_offset: LogletOffset,
        status: Option<RecordStatus>,
    }

    impl From<super::DigestEntry> for DigestEntry {
        fn from(value: super::DigestEntry) -> Self {
            let super::DigestEntry {
                from_offset,
                to_offset,
                status,
            } = value;

            Self {
                from_offset,
                to_offset,
                status: Some(status),
            }
        }
    }

    impl TryFrom<DigestEntry> for super::DigestEntry {
        type Error = ConversionError;

        fn try_from(value: DigestEntry) -> Result<Self, Self::Error> {
            let DigestEntry {
                from_offset,
                to_offset,
                status,
            } = value;

            Ok(Self {
                from_offset,
                to_offset,
                status: status.ok_or_else(|| ConversionError::missing_field("status"))?,
            })
        }
    }

    /// Response to a `GetDigest` request
    #[derive(Debug, Clone, bilrost::Message)]
    pub struct Digest {
        #[bilrost(1)]
        header: LogServerResponseHeader,
        #[bilrost(2)]
        entries: Vec<DigestEntry>,
    }

    impl From<super::Digest> for Digest {
        fn from(value: super::Digest) -> Self {
            let super::Digest { header, entries } = value;

            Self {
                header: header.into(),
                entries: entries.into_iter().map(Into::into).collect(),
            }
        }
    }

    impl TryFrom<Digest> for super::Digest {
        type Error = ConversionError;

        fn try_from(value: Digest) -> Result<Self, Self::Error> {
            let Digest { header, entries } = value;

            Ok(Self {
                header: header.try_into()?,
                entries: {
                    let mut result = Vec::with_capacity(entries.len());
                    for entry in entries {
                        result.push(entry.try_into()?);
                    }
                    result
                },
            })
        }
    }
}
