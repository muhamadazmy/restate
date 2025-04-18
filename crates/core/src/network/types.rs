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
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use opentelemetry::Context;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use restate_types::net::codec::{Targeted, WireEncode};
use restate_types::net::{AdvertisedAddress, RpcRequest};
use restate_types::{GenerationalNodeId, NodeId, Version};

use super::protobuf::network::Header;
use super::{Connection, ConnectionClosed, NetworkSendError};

static NEXT_MSG_ID: AtomicU64 = const { AtomicU64::new(1) };

/// Address of a peer in the network. It can be a specific node or an anonymous peer.
#[derive(Debug, Clone, Copy, Eq, derive_more::IsVariant, derive_more::Display)]
pub enum PeerAddress {
    #[display("{_0}")]
    ServerNode(GenerationalNodeId),
    #[display("Anonymous")]
    Anonymous,
}

impl PartialEq for PeerAddress {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::ServerNode(l0), Self::ServerNode(r0)) => l0 == r0,
            // anonymous peers are not comparable (partial equivalence)
            _ => false,
        }
    }
}

#[derive(Hash, Eq, PartialEq, Clone, Debug, derive_more::Display)]
pub enum Destination {
    Address(AdvertisedAddress),
    Node(GenerationalNodeId),
}

/// generate a new unique message id for this node
#[inline(always)]
pub(crate) fn generate_msg_id() -> u64 {
    NEXT_MSG_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

// Using type-state pattern to model Outgoing
#[derive(Debug)]
pub struct HasConnection(Connection);

#[derive(Debug)]
pub struct NoConnection(NodeId);

pub(super) mod private {
    use super::*;

    // Make sure that NetworkSender can be implemented on this set of types only.
    pub trait Sealed {}
    impl Sealed for HasConnection {}
    impl Sealed for NoConnection {}
}

#[derive(Debug, Clone)]
struct MsgMeta {
    msg_id: u64,
    in_response_to: Option<u64>,
}

#[derive(Clone, Debug, Copy, Default)]
pub struct PeerMetadataVersion {
    pub logs: Option<Version>,
    pub nodes_config: Option<Version>,
    pub partition_table: Option<Version>,
    pub schema: Option<Version>,
}

impl From<Header> for PeerMetadataVersion {
    fn from(value: Header) -> Self {
        Self {
            logs: value.my_logs_version.map(Version::from),
            nodes_config: value.my_nodes_config_version.map(Version::from),
            partition_table: value.my_partition_table_version.map(Version::from),
            schema: value.my_schema_version.map(Version::from),
        }
    }
}
/// A wrapper for incoming messages that includes the sender information
#[derive(Debug, Clone)]
pub struct Incoming<M> {
    meta: MsgMeta,
    connection: Connection,
    body: M,
    metadata_version: PeerMetadataVersion,
    parent_context: Option<Context>,
}

impl<M> Incoming<M> {
    pub(crate) fn from_parts(
        body: M,
        connection: Connection,
        msg_id: u64,
        in_response_to: Option<u64>,
        metadata_version: PeerMetadataVersion,
    ) -> Self {
        Self {
            connection,
            body,
            meta: MsgMeta {
                msg_id,
                in_response_to,
            },
            metadata_version,
            parent_context: None,
        }
    }

    pub(crate) fn with_parent_context(mut self, context: Option<Context>) -> Self {
        self.parent_context = context;
        self
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn for_testing(connection: Connection, body: M, in_response_to: Option<u64>) -> Self {
        let msg_id = generate_msg_id();
        Self::from_parts(
            body,
            connection,
            msg_id,
            in_response_to,
            PeerMetadataVersion::default(),
        )
    }

    /// Returns an open telemetry Context which is
    /// traced over from the sender of the message
    pub fn parent_context(&self) -> Option<&Context> {
        self.parent_context.as_ref()
    }

    /// A shortcut to set current tracing [`Span`] parent
    /// to remote caller span context
    ///
    /// This only works on the first call. Subsequent calls
    /// has no effect on the current [`Span`].
    ///
    /// If you need to create `parallel` spans for the same
    /// incoming message, use [`Self::parent_context()`] instead
    pub fn follow_from_sender(&mut self) {
        if let Some(context) = self.parent_context.take() {
            Span::current().set_parent(context)
        }
    }

    /// A shortcut to set given [`Span`] parent
    /// to remote caller span context
    ///
    /// This only works on the first call. Subsequent calls
    /// has no effect on the current [`Span`].
    ///
    /// If you need to create `parallel` spans for the same
    /// incoming message, use [`Self::parent_context()`] instead
    pub fn follow_from_sender_for(&mut self, span: &Span) {
        if let Some(context) = self.parent_context.take() {
            span.set_parent(context)
        }
    }

    pub fn peer(&self) -> GenerationalNodeId {
        self.connection.peer()
    }

    pub fn into_body(self) -> M {
        self.body
    }

    pub fn body(&self) -> &M {
        &self.body
    }

    pub fn msg_id(&self) -> u64 {
        self.meta.msg_id
    }

    pub fn in_response_to(&self) -> Option<u64> {
        self.meta.in_response_to
    }

    pub fn metadata_version(&self) -> &PeerMetadataVersion {
        &self.metadata_version
    }

    pub fn try_map<O, E>(self, f: impl FnOnce(M) -> Result<O, E>) -> Result<Incoming<O>, E> {
        Ok(Incoming {
            connection: self.connection,
            body: f(self.body)?,
            meta: self.meta,
            metadata_version: self.metadata_version,
            parent_context: self.parent_context,
        })
    }

    pub fn map<O>(self, f: impl FnOnce(M) -> O) -> Incoming<O> {
        Incoming {
            connection: self.connection,
            body: f(self.body),
            meta: self.meta,
            metadata_version: self.metadata_version,
            parent_context: self.parent_context,
        }
    }

    /// Create an [`Outgoing`] to respond to this request.
    ///
    /// Sending this outgoing will reuse the same connection where this message arrived
    pub fn into_outgoing<O>(self, body: O) -> Outgoing<O, HasConnection> {
        let reciprocal = Reciprocal::new(self.connection, self.meta.msg_id);
        reciprocal.prepare(body)
    }
}

/// Only available if this in RpcRequest for convenience.
impl<M: RpcRequest> Incoming<M> {
    pub fn to_rpc_response(
        self,
        response: M::ResponseMessage,
    ) -> Outgoing<M::ResponseMessage, HasConnection> {
        self.into_outgoing(response)
    }

    /// Dissolve this incoming into [`Reciprocal`] which can be used to prepare responses, and the
    /// body of this incoming message.
    pub fn split(self) -> (Reciprocal<M::ResponseMessage>, M) {
        let reciprocal = Reciprocal::<M::ResponseMessage>::new(self.connection, self.meta.msg_id);
        (reciprocal, self.body)
    }

    /// Creates a reciprocal for this incoming message without consuming it. This will internally
    /// clone the original connection reference.
    pub fn create_reciprocal(&self) -> Reciprocal<M::ResponseMessage> {
        Reciprocal::<M::ResponseMessage>::new(self.connection.clone(), self.meta.msg_id)
    }
}

/// A type that represents a potential response (reciprocal to a request) that can be converted
/// into `Outgoing` once a message is ready. An [`Outgoing`] can be created with `prepare(body)`
#[derive(Debug)]
pub struct Reciprocal<O> {
    connection: Connection,
    in_response_to: u64,
    _phantom: PhantomData<O>,
}

impl<O> Reciprocal<O> {
    pub(crate) fn new(connection: Connection, in_response_to: u64) -> Self {
        Self {
            connection,
            in_response_to,
            _phantom: PhantomData,
        }
    }

    pub fn peer(&self) -> GenerationalNodeId {
        self.connection.peer()
    }

    /// Package this reciprocal as a ready-to-use Outgoing message that holds the connection
    /// reference and the original message_id to response to.
    pub fn prepare(self, body: O) -> Outgoing<O, HasConnection> {
        Outgoing {
            connection: HasConnection(self.connection),
            body,
            meta: MsgMeta {
                msg_id: generate_msg_id(),
                in_response_to: Some(self.in_response_to),
            },
        }
    }
}

/// A wrapper for outgoing messages that includes the correlation information if a message is in
/// response to a request.
#[derive(Debug, Clone)]
pub struct Outgoing<M, State = NoConnection> {
    connection: State,
    body: M,
    meta: MsgMeta,
}

impl<M: Targeted + WireEncode> Outgoing<M, NoConnection> {
    pub fn new(peer: impl Into<NodeId>, body: M) -> Self {
        Outgoing {
            connection: NoConnection(peer.into()),
            body,
            meta: MsgMeta {
                msg_id: generate_msg_id(),
                in_response_to: None,
            },
        }
    }
}

impl<M, S> Outgoing<M, S> {
    pub fn into_body(self) -> M {
        self.body
    }

    pub fn body(&self) -> &M {
        &self.body
    }

    pub fn body_mut(&mut self) -> &mut M {
        &mut self.body
    }

    pub fn msg_id(&self) -> u64 {
        self.meta.msg_id
    }

    pub fn in_response_to(&self) -> Option<u64> {
        self.meta.in_response_to
    }

    pub fn try_map<O, E>(self, f: impl FnOnce(M) -> Result<O, E>) -> Result<Outgoing<O, S>, E> {
        Ok(Outgoing {
            connection: self.connection,
            body: f(self.body)?,
            meta: self.meta,
        })
    }

    pub fn map<O>(self, f: impl FnOnce(M) -> O) -> Outgoing<O, S> {
        Outgoing {
            connection: self.connection,
            body: f(self.body),
            meta: self.meta,
        }
    }
}

/// Only available if this outgoing is pinned to a connection
impl<M> Outgoing<M, HasConnection> {
    pub fn peer(&self) -> GenerationalNodeId {
        self.connection.0.peer()
    }

    /// Unpins this outgoing from the connection. Note that the outgoing will still be pinned the
    /// specific GenerationalNodeId originally associated with the connection. If you want to unset
    /// the peer to use any generation. Call `to_any_generation()` on the returned Outgoing value.
    pub fn forget_connection(self) -> Outgoing<M, NoConnection> {
        Outgoing {
            connection: NoConnection(self.peer().into()),
            body: self.body,
            meta: self.meta,
        }
    }
}

/// Only available if this outgoing is **not** pinned to a connection
impl<M> Outgoing<M, NoConnection> {
    pub fn peer(&self) -> &NodeId {
        &self.connection.0
    }

    pub fn set_peer(self, peer: NodeId) -> Self {
        Self {
            connection: NoConnection(peer),
            ..self
        }
    }

    /// Ensures that this outgoing is not pinned to a specific node generation.
    pub fn to_any_generation(self) -> Self {
        Self {
            connection: NoConnection(self.peer().id().into()),
            ..self
        }
    }

    /// Panics (debug assertion) if connection doesn't match the plain node Id of the original message
    pub fn assign_connection(self, connection: Connection) -> Outgoing<M, HasConnection> {
        debug_assert_eq!(self.connection.0.id(), connection.peer().as_plain());
        Outgoing {
            connection: HasConnection(connection),
            body: self.body,
            meta: self.meta,
        }
    }
}

impl<M: Targeted + WireEncode> Outgoing<M, HasConnection> {
    /// Send a message on this connection.
    ///
    /// This blocks until there is capacity on the connection stream.
    ///
    /// This returns Ok(()) when the message is:
    /// - Successfully serialized to the wire format based on the negotiated protocol
    /// - Serialized message was enqueued on the send buffer of the socket
    ///
    /// That means that this is not a guarantee that the message has been sent
    /// over the network or that the peer has received it.
    ///
    /// If this is needed, the caller must design the wire protocol with a
    /// request/response state machine and perform retries on other nodes/connections if needed.
    ///
    /// This roughly maps to the semantics of a POSIX write/send socket operation.
    ///
    /// This doesn't auto-retry connection resets or send errors, this is up to the user
    /// for retrying externally.
    // #[instrument(level = "trace", skip_all, fields(peer_node_id = %self.peer, target_service = ?message.target(), msg = ?message.kind()))]
    pub async fn send(self) -> Result<(), NetworkSendError<Self>> {
        let connection = &self.connection.0;
        let permit = match tokio::task::unconstrained(connection.reserve_owned()).await {
            Some(permit) => permit,
            None => return Err(NetworkSendError::new(self, ConnectionClosed.into())),
        };

        permit.send(self);
        Ok(())
    }

    /// Sends a message with timeout limit. Returns [`NetworkError::Timeout`] if deadline exceeded while waiting for capacity
    /// on the assigned connection or returns [`NetworkError::ConnectionClosed`] immediately if the
    /// assigned connection is no longer valid.
    pub async fn send_timeout(self, timeout: Duration) -> Result<(), NetworkSendError<Self>> {
        let connection = &self.connection.0;
        let permit = match tokio::task::unconstrained(connection.reserve_timeout(timeout)).await {
            Ok(permit) => permit,
            Err(e) => return Err(NetworkSendError::new(self, e)),
        };

        permit.send(self);
        Ok(())
    }

    /// Sends a response on the same connection where we received the request.
    /// Returns `true` if successful
    pub fn try_send(self) -> bool {
        let connection = &self.connection.0;
        let Some(permit) = connection.try_reserve_owned() else {
            return false;
        };

        permit.send(self);

        true
    }
}
