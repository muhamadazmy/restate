// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
    str::FromStr,
    sync::{Arc, LazyLock},
};

use axum::{
    body::Bytes,
    extract::{Path, State},
    response::{AppendHeaders, IntoResponse, Response},
    routing::{delete, get, head, put},
    Router,
};
use bytestring::ByteString;
use http::{header::ToStrError, HeaderMap, StatusCode};

use restate_core::metadata_store::{MetadataStore, VersionedValue};
use restate_metadata_store::{MetadataStoreClient, Precondition, ReadError, WriteError};
use restate_types::{metadata_store::keys, Version};

/// Version header.
const HEADER_VERSION: &str = "X-Version";

/// Precondition header. Possible values are
/// - none: forces the operation
/// - does-not-exist: key must not exist
/// - <version>: Match given version number
const HEADER_PRECONDITION: &str = "X-Precondition";

static PROTECTED_KEYS: LazyLock<HashSet<ByteString>> = LazyLock::new(|| {
    [
        keys::PARTITION_TABLE_KEY.clone(),
        keys::BIFROST_CONFIG_KEY.clone(),
        keys::NODES_CONFIG_KEY.clone(),
        keys::SCHEMA_INFORMATION_KEY.clone(),
    ]
    .into()
});

fn is_protected(key: &ByteString) -> bool {
    PROTECTED_KEYS.contains(key) || key.starts_with(keys::PARTITION_PROCESSOR_EPOCH_PREFIX)
}

#[derive(Clone, derive_more::Deref)]
struct MetadataStoreState {
    inner: Arc<dyn MetadataStore + Send + Sync>,
}

pub fn router(metadata_store_client: &MetadataStoreClient) -> Router {
    let state = MetadataStoreState {
        inner: metadata_store_client.inner(),
    };

    Router::new()
        .route("/metadata/:key", get(get_key))
        .route("/metadata/:key", head(get_key_version))
        .route("/metadata/:key", put(put_key))
        .route("/metadata/:key", delete(delete_key))
        .with_state(state)
}

/// Deletes a key from the metadata store
///
/// Requires `X-Precondition` header
async fn delete_key(
    Path(key): Path<String>,
    State(store): State<MetadataStoreState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, StoreApiError> {
    let key = key.into();

    if is_protected(&key) {
        return Err(StoreApiError::ProtectedKey);
    }

    // todo(azmy): implement both PreconditionHeader and VersionHeader
    // as extractors after updating axium to version > 0.8
    let precondition: PreconditionHeader = headers
        .get(HEADER_PRECONDITION)
        .ok_or(PreconditionError::MissingPrecondition)?
        .to_str()?
        .parse()?;

    store.delete(key, precondition.into()).await?;
    Ok(())
}

/// Puts a key/value pair
///
/// Requires `X-Precondition` header
/// Required `X-Version` header
async fn put_key(
    Path(key): Path<String>,
    State(store): State<MetadataStoreState>,
    headers: HeaderMap,
    value: Bytes,
) -> Result<impl IntoResponse, StoreApiError> {
    // todo(azmy): implement both PreconditionHeader and VersionHeader
    // as extractors after updating axium to version > 0.8
    let key = key.into();

    if is_protected(&key) {
        return Err(StoreApiError::ProtectedKey);
    }

    let precondition: PreconditionHeader = headers
        .get(HEADER_PRECONDITION)
        .ok_or(PreconditionError::MissingPrecondition)?
        .to_str()?
        .parse()?;

    let version: VersionHeader = headers
        .get(HEADER_VERSION)
        .ok_or(VersionError::MissingVersion)?
        .to_str()?
        .parse()?;

    let versioned = VersionedValue {
        version: version.into(),
        value,
    };

    store.put(key, versioned, precondition.into()).await?;
    Ok(())
}

/// Gets a key from the metadata store
async fn get_key(
    Path(key): Path<String>,
    State(store): State<MetadataStoreState>,
) -> Result<impl IntoResponse, StoreApiError> {
    let value = store
        .get(key.into())
        .await?
        .ok_or(StoreApiError::NotFound)?;

    Ok(VersionedValueResponse::from(value))
}

/// Gets a key version from the metadata store
async fn get_key_version(
    Path(key): Path<String>,
    State(store): State<MetadataStoreState>,
) -> Result<impl IntoResponse, HeadError> {
    let version = store
        .get_version(key.into())
        .await
        .map_err(StoreApiError::from)?
        .ok_or(StoreApiError::NotFound)?;

    Ok(AppendHeaders([(HEADER_VERSION, version.to_string())]))
}

#[derive(Debug, thiserror::Error)]
enum StoreApiError {
    #[error(transparent)]
    ReadError(#[from] ReadError),
    #[error(transparent)]
    WriteError(#[from] WriteError),
    #[error("Key not found")]
    NotFound,

    #[error(transparent)]
    ToStrError(#[from] ToStrError),
    #[error(transparent)]
    PreconditionError(#[from] PreconditionError),
    #[error(transparent)]
    VersionError(#[from] VersionError),
    #[error("Key is protected")]
    ProtectedKey,
}

impl StoreApiError {
    fn code(&self) -> StatusCode {
        match self {
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::WriteError(WriteError::FailedPrecondition(_)) => StatusCode::PRECONDITION_FAILED,
            Self::ProtectedKey => StatusCode::UNAUTHORIZED,
            Self::ReadError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::WriteError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::ToStrError(_) | Self::PreconditionError(_) | Self::VersionError(_) => {
                StatusCode::BAD_REQUEST
            }
        }
    }
}

impl IntoResponse for StoreApiError {
    fn into_response(self) -> Response {
        (self.code(), self.to_string()).into_response()
    }
}

// We can't return a body as a response to Head requests
#[derive(derive_more::From)]
struct HeadError(StoreApiError);

impl IntoResponse for HeadError {
    fn into_response(self) -> Response {
        self.0.code().into_response()
    }
}

#[derive(derive_more::From)]
struct VersionedValueResponse(VersionedValue);

impl IntoResponse for VersionedValueResponse {
    fn into_response(self) -> Response {
        (
            AppendHeaders([(HEADER_VERSION, u32::from(self.0.version).to_string())]),
            self.0.value,
        )
            .into_response()
    }
}

#[derive(Debug, thiserror::Error)]
enum PreconditionError {
    #[error("Missing precondition")]
    MissingPrecondition,
    #[error("Invalid precondition")]
    InvalidPrecondition,
}

struct PreconditionHeader(Precondition);

impl FromStr for PreconditionHeader {
    type Err = PreconditionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let inner = match s.to_lowercase().as_str() {
            "none" => Precondition::None,
            "does-not-exist" => Precondition::DoesNotExist,
            version => {
                let version: u32 = version
                    .parse()
                    .map_err(|_| PreconditionError::InvalidPrecondition)?;

                Precondition::MatchesVersion(version.into())
            }
        };

        Ok(Self(inner))
    }
}

impl From<PreconditionHeader> for Precondition {
    fn from(value: PreconditionHeader) -> Self {
        value.0
    }
}

#[derive(Debug, thiserror::Error)]
enum VersionError {
    #[error("Missing version")]
    MissingVersion,

    #[error("Invalid version")]
    InvalidVersion,
}
struct VersionHeader(Version);

impl FromStr for VersionHeader {
    type Err = VersionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let version: u32 = s.parse().map_err(|_| VersionError::InvalidVersion)?;

        Ok(VersionHeader(version.into()))
    }
}

impl From<VersionHeader> for Version {
    fn from(value: VersionHeader) -> Self {
        value.0
    }
}
