// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use tonic::Code;

use restate_types::GenerationalNodeId;
use restate_types::net::MIN_SUPPORTED_PROTOCOL_VERSION;
use restate_types::nodes_config::NodesConfigError;

use crate::{ShutdownError, SyncError};

#[derive(Debug, thiserror::Error)]
#[error("connection closed")]
pub struct ConnectionClosed;

#[derive(Debug, thiserror::Error)]
pub enum RouterError {
    #[error("target not registered: {0}")]
    NotRegisteredTarget(String),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[derive(Debug, thiserror::Error)]
#[error("send error: {source}")]
pub struct NetworkSendError<M> {
    pub original: M,
    #[source]
    pub source: NetworkError,
}

impl<M> NetworkSendError<M> {
    pub fn new(original: M, source: NetworkError) -> Self {
        Self { original, source }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum NetworkError {
    #[error("unknown node: {0}")]
    UnknownNode(#[from] NodesConfigError),
    #[error("operation aborted, node is shutting down")]
    Shutdown(#[from] ShutdownError),
    #[error("exceeded deadline after spending {0:?}")]
    Timeout(Duration),
    #[error("protocol error: {0}")]
    ProtocolError(#[from] ProtocolError),
    #[error("cannot connect: {} {}", tonic::Status::code(.0), tonic::Status::message(.0))]
    ConnectError(tonic::Status),
    #[error("new node generation exists: {0}")]
    OldPeerGeneration(String),
    #[error("node {0} was shut down")]
    NodeIsGone(GenerationalNodeId),
    #[error(transparent)]
    ConnectionClosed(#[from] ConnectionClosed),
    #[error("cannot send messages to this node: {0}")]
    Unavailable(String),
    #[error("failed syncing metadata: {0}")]
    Metadata(#[from] SyncError),
    #[error("remote metadata version mismatch: {0}")]
    // todo(azmy): A temporary error that should be removed
    // after relaxing the restrictions on node ids in upcoming change
    RemoteVersionMismatch(String),
    #[error("network channel is full and sending would block")]
    Full,
}

impl From<tonic::Status> for NetworkError {
    fn from(value: tonic::Status) -> Self {
        if value.code() == Code::FailedPrecondition {
            Self::RemoteVersionMismatch(value.message().into())
        } else {
            Self::ConnectError(value)
        }
    }
}
#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("handshake failed: {0}")]
    HandshakeFailed(&'static str),
    #[error("handshake timeout: {0}")]
    HandshakeTimeout(&'static str),
    #[error("peer dropped connection")]
    PeerDropped,
    #[error("grpc error: {0}")]
    GrpcError(#[from] tonic::Status),
    #[error(
        "peer has unsupported protocol version {0}, minimum supported is '{p}'",
        p = MIN_SUPPORTED_PROTOCOL_VERSION as i32
    )]
    UnsupportedVersion(i32),
}

impl From<ProtocolError> for tonic::Status {
    fn from(value: ProtocolError) -> Self {
        match value {
            ProtocolError::HandshakeFailed(e) => tonic::Status::invalid_argument(e),
            ProtocolError::HandshakeTimeout(e) => tonic::Status::deadline_exceeded(e),
            ProtocolError::PeerDropped => tonic::Status::cancelled("peer dropped"),
            ProtocolError::UnsupportedVersion(_) => {
                tonic::Status::invalid_argument(value.to_string())
            }
            ProtocolError::GrpcError(s) => s,
        }
    }
}

impl From<NetworkError> for tonic::Status {
    fn from(value: NetworkError) -> Self {
        match value {
            NetworkError::Shutdown(_) => tonic::Status::unavailable(value.to_string()),
            NetworkError::ProtocolError(e) => e.into(),
            NetworkError::Timeout(_) => tonic::Status::deadline_exceeded(value.to_string()),
            NetworkError::OldPeerGeneration(e) => tonic::Status::already_exists(e),
            NetworkError::NodeIsGone(_) => tonic::Status::already_exists(value.to_string()),
            NetworkError::ConnectError(s) => s,
            NetworkError::UnknownNode(err @ NodesConfigError::GenerationMismatch { .. }) => {
                tonic::Status::failed_precondition(err.to_string())
            }
            e => tonic::Status::internal(e.to_string()),
        }
    }
}
