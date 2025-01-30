// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{collections::HashMap, fmt::Display, future::Future, sync::Arc};

use cling::{prelude::Parser, Collect};
use itertools::Either;
use restate_core::protobuf::node_ctl_svc::{
    node_ctl_svc_client::NodeCtlSvcClient, GetMetadataRequest,
};
use restate_types::{
    net::AdvertisedAddress,
    nodes_config::{NodesConfiguration, Role},
    protobuf::common::MetadataKind,
    storage::{StorageCodec, StorageDecodeError},
    PlainNodeId, Version,
};
use tokio::sync::Mutex;
use tonic::{codec::CompressionEncoding, transport::Channel, Response, Status};

use crate::util::grpc_channel;

#[derive(Parser, Collect, Debug, Clone)]
pub struct ConnectionInfo {
    // todo: rename this to be a node address for reusability across commands
    /// Cluster Controller address
    #[clap(
        long="address",
        value_hint = clap::ValueHint::Url,
        default_value = "http://localhost:5122/",
        env = "RESTATE_ADDRESS",
        global = true
    )]
    pub addresses: Vec<AdvertisedAddress>,

    /// Sync metadata from metadata store first
    #[arg(long)]
    pub sync_metadata: bool,

    #[clap(skip)]
    nodes_configuration: Arc<Mutex<Option<NodesConfiguration>>>,

    #[clap(skip)]
    channels: Arc<Mutex<HashMap<PlainNodeId, Channel>>>,
}

impl ConnectionInfo {
    pub fn clients(
        &self,
    ) -> impl Iterator<Item = (AdvertisedAddress, NodeCtlSvcClient<Channel>)> + use<'_> {
        self.addresses.iter().cloned().map(|address| {
            let channel = grpc_channel(address.clone());
            (
                address,
                NodeCtlSvcClient::new(channel)
                    .accept_compressed(CompressionEncoding::Gzip)
                    .send_compressed(CompressionEncoding::Gzip),
            )
        })
    }

    pub async fn get_nodes_configuration(&self) -> Result<NodesConfiguration, ConnectionInfoError> {
        let mut config_guard = self.nodes_configuration.lock().await;
        if let Some(config) = &*config_guard {
            return Ok(config.clone());
        }

        let mut latest_configuration: Option<NodesConfiguration> = None;
        let mut answer = false;
        let mut errors = NodesErrors::default();
        for (address, mut client) in self.clients() {
            let req = GetMetadataRequest {
                kind: MetadataKind::NodesConfiguration.into(),
                sync: false,
            };

            let mut response = match client.get_metadata(req).await {
                Ok(response) => response.into_inner(),
                Err(status) => {
                    errors.error(address, status);
                    continue;
                }
            };

            let nodes_configuration =
                StorageCodec::decode::<NodesConfiguration, _>(&mut response.encoded)
                    .map_err(|err| ConnectionInfoError::InvalidNodesConfiguration(address, err))?;

            answer = true;
            if nodes_configuration.version()
                > latest_configuration
                    .as_ref()
                    .map(|c| c.version())
                    .unwrap_or(Version::INVALID)
            {
                latest_configuration = Some(nodes_configuration);
            }
        }

        if !answer {
            // got no answer from all the nodes.
            return Err(ConnectionInfoError::NodesErrors(errors));
        }

        *config_guard = latest_configuration.clone();
        latest_configuration.ok_or(ConnectionInfoError::MissingNodesConfiguration)
    }

    pub async fn try_each<F, T, Fut>(
        &self,
        role: Option<Role>,
        mut closure: F,
    ) -> Result<Response<T>, ConnectionInfoError>
    where
        F: FnMut(Channel) -> Fut,
        Fut: Future<Output = Result<Response<T>, Status>>,
    {
        let nodes_config = self.get_nodes_configuration().await?;
        let iterator = match role {
            Some(role) => Either::Left(nodes_config.iter_role(role)),
            None => Either::Right(nodes_config.iter()),
        };

        let mut errors = NodesErrors::new(role);
        for (node_id, node) in iterator {
            // avoid creating new channels on each iteration. Instead cheaply copy the channels
            let mut channels = self.channels.lock().await;
            let channel = channels
                .entry(node_id)
                .or_insert_with(|| grpc_channel(node.address.clone()));

            let result = closure(channel.clone()).await;
            match result {
                Ok(response) => return Ok(response),
                Err(status) => {
                    errors.error(node.address.clone(), status);
                }
            }
        }

        Err(ConnectionInfoError::NodesErrors(errors))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConnectionInfoError {
    #[error("Could not retrieve nodes configuration. Possible un provisioned cluster!")]
    MissingNodesConfiguration,

    #[error("Failed to decode nodes configuration from node {0}: {1}")]
    InvalidNodesConfiguration(AdvertisedAddress, StorageDecodeError),

    #[error(transparent)]
    NodesErrors(NodesErrors),
}

#[derive(Debug, Default)]
pub struct NodesErrors {
    role: Option<Role>,
    node_status: Vec<(AdvertisedAddress, Status)>,
}

impl NodesErrors {
    fn new(role: Option<Role>) -> Self {
        Self {
            role,
            node_status: Vec::default(),
        }
    }

    fn error(&mut self, node: AdvertisedAddress, status: Status) {
        self.node_status.push((node, status));
    }
}

impl Display for NodesErrors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.node_status.is_empty() {
            match self.role {
                Some(role) => {
                    write!(f, "No available {role} nodes to satisfy the request")?;
                }
                None => {
                    write!(f, "No available nodes to satisfy the request")?;
                }
            }
            return Ok(());
        }

        writeln!(
            f,
            "Encountered multiple errors trying to retrieve nodes configurations:"
        )?;
        for (address, status) in &self.node_status {
            writeln!(f, " - {address} -> {status}")?;
        }
        Ok(())
    }
}

impl std::error::Error for NodesErrors {
    fn description(&self) -> &str {
        "aggregated nodes error"
    }
}
