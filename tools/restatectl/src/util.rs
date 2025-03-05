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
    fmt::{self, Display},
    num::ParseIntError,
    ops::RangeInclusive,
    str::FromStr,
};

use bytes::BytesMut;
use cling::{Collect, prelude::Parser};
use tonic::{Code, Status, codec::CompressionEncoding, transport::Channel};

use restate_cli_util::CliContext;
use restate_core::{
    network::net_util::create_tonic_channel,
    protobuf::metadata_proxy_svc::{
        GetRequest, PutRequest, metadata_proxy_svc_client::MetadataProxySvcClient,
    },
};
use restate_types::{
    Versioned,
    errors::GenericError,
    logs::metadata::ProviderConfiguration,
    metadata::Precondition,
    net::AdvertisedAddress,
    protobuf::metadata::VersionedValue,
    storage::{StorageCodec, StorageDecode, StorageEncode},
};

use crate::connection::NodeOperationError;

pub fn grpc_channel(address: AdvertisedAddress) -> Channel {
    let ctx = CliContext::get();
    create_tonic_channel(address, &ctx.network)
}

pub fn write_default_provider<W: fmt::Write>(
    w: &mut W,
    depth: usize,
    provider: &ProviderConfiguration,
) -> Result<(), fmt::Error> {
    let title = "Logs Provider";
    match provider {
        #[cfg(any(test, feature = "memory-loglet"))]
        ProviderConfiguration::InMemory => {
            write_leaf(w, depth, true, title, "in-memory")?;
        }
        ProviderConfiguration::Local => {
            write_leaf(w, depth, true, title, "local")?;
        }
        #[cfg(feature = "replicated-loglet")]
        ProviderConfiguration::Replicated(config) => {
            write_leaf(w, depth, true, title, "replicated")?;
            let depth = depth + 1;
            write_leaf(
                w,
                depth,
                false,
                "Log replication",
                config.replication_property.to_string(),
            )?;
            write_leaf(
                w,
                depth,
                true,
                "Nodeset size",
                config.target_nodeset_size.to_string(),
            )?;
        }
    }
    Ok(())
}

pub fn write_leaf<W: fmt::Write>(
    w: &mut W,
    depth: usize,
    last: bool,
    title: impl Display,
    value: impl Display,
) -> Result<(), fmt::Error> {
    let depth = depth + 1;
    let chr = if last { '└' } else { '├' };
    writeln!(w, "{chr:>depth$} {title}: {value}")
}

#[derive(Parser, Collect, Clone, Debug)]
pub struct RangeParam {
    from: u32,
    to: u32,
}

impl RangeParam {
    fn new(from: u32, to: u32) -> Result<Self, RangeParamError> {
        if from > to {
            Err(RangeParamError::InvalidRange(from, to))
        } else {
            Ok(RangeParam { from, to })
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = u32> {
        self.from..=self.to
    }
}

impl<T> From<T> for RangeParam
where
    T: Into<u32>,
{
    fn from(value: T) -> Self {
        let id = value.into();
        Self::new(id, id).expect("equal values")
    }
}

impl IntoIterator for RangeParam {
    type IntoIter = RangeInclusive<u32>;
    type Item = u32;
    fn into_iter(self) -> Self::IntoIter {
        self.from..=self.to
    }
}

impl IntoIterator for &RangeParam {
    type IntoIter = RangeInclusive<u32>;
    type Item = u32;
    fn into_iter(self) -> Self::IntoIter {
        self.from..=self.to
    }
}

impl FromStr for RangeParam {
    type Err = RangeParamError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split("-").collect();
        match parts.len() {
            1 => {
                let n = parts[0].parse()?;
                Ok(RangeParam::new(n, n)?)
            }
            2 => {
                let from = parts[0].parse()?;
                let to = parts[1].parse()?;
                Ok(RangeParam::new(from, to)?)
            }
            _ => Err(RangeParamError::InvalidSyntax(s.to_string())),
        }
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum RangeParamError {
    #[error("Invalid id range: {0}..{1} start must be <= end range")]
    InvalidRange(u32, u32),
    #[error("Invalid range syntax '{0}'")]
    InvalidSyntax(String),
    #[error(transparent)]
    ParseError(#[from] ParseIntError),
}

pub async fn read_modify_write<F, T, E>(
    channel: &mut Channel,
    key: impl Into<String>,
    mut modify: F,
) -> Result<(), ReadModifyWriteError<E>>
where
    F: FnMut(Option<T>) -> Result<T, E>,
    E: Display,
    T: Versioned + StorageEncode + StorageDecode,
{
    let mut client = MetadataProxySvcClient::new(channel)
        .accept_compressed(CompressionEncoding::Gzip)
        .send_compressed(CompressionEncoding::Gzip);

    let key = key.into();

    let mut buf = BytesMut::new();
    loop {
        let old_value: Option<T> = client
            .get(GetRequest { key: key.clone() })
            .await?
            .into_inner()
            .value
            .map(|mut value| StorageCodec::decode(&mut value.bytes))
            .transpose()
            .map_err(|err| ReadModifyWriteError::Codec(err.into()))?;

        let precondition = old_value
            .as_ref()
            .map(|v| Precondition::MatchesVersion(v.version()))
            .unwrap_or(Precondition::DoesNotExist);

        let new_value = match modify(old_value) {
            Ok(value) => value,
            Err(err) => return Err(ReadModifyWriteError::OperationFailed(err)),
        };

        StorageCodec::encode(&new_value, &mut buf)
            .map_err(|err| ReadModifyWriteError::Codec(err.into()))?;

        let versioned_value = VersionedValue {
            version: Some(new_value.version().into()),
            bytes: buf.split().into(),
        };

        let request = PutRequest {
            key: key.clone(),
            precondition: Some(precondition.into()),
            value: Some(versioned_value),
        };

        return match client.put(request).await {
            Err(status) if status.code() == Code::FailedPrecondition => {
                continue;
            }
            Err(status) => Err(status.into()),
            Ok(_) => Ok(()),
        };
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ReadModifyWriteError<E: Display> {
    #[error(transparent)]
    Codec(GenericError),
    #[error(transparent)]
    RemoteError(Status),
    #[error("failed read-modify-write operation: {0}")]
    OperationFailed(E),
}

impl<E: Display> From<Status> for ReadModifyWriteError<E> {
    fn from(value: Status) -> Self {
        Self::RemoteError(value)
    }
}

impl<E: Display> From<ReadModifyWriteError<E>> for NodeOperationError {
    fn from(value: ReadModifyWriteError<E>) -> Self {
        match value {
            ReadModifyWriteError::Codec(err) => Self::Terminal(Status::unknown(err.to_string())),
            ReadModifyWriteError::OperationFailed(err) => {
                Self::Terminal(Status::unknown(format!("Update operation failed: {err}")))
            }
            ReadModifyWriteError::RemoteError(status) => Self::RetryableElsewhere(status),
        }
    }
}
