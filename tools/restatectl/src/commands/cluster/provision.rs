// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::commands::cluster::config::cluster_config_string;
use crate::connection::ConnectionInfo;
use crate::util::grpc_channel;
use clap::Parser;
use cling::{Collect, Run};
use restate_cli_util::ui::console::confirm_or_exit;
use restate_cli_util::{c_error, c_println, c_warn};
use restate_core::protobuf::node_ctl_svc::node_ctl_svc_client::NodeCtlSvcClient;
use restate_core::protobuf::node_ctl_svc::ProvisionClusterRequest;
use restate_types::logs::metadata::{ProviderConfiguration, ProviderKind, ReplicatedLogletConfig};
use restate_types::replication::ReplicationProperty;
use std::cmp::Ordering;
use std::num::NonZeroU16;
use tonic::codec::CompressionEncoding;
use tonic::Code;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "cluster_provision")]
pub struct ProvisionOpts {
    /// Number of partitions
    #[clap(long)]
    num_partitions: Option<NonZeroU16>,

    /// Optional partition placement strategy. By default replicates
    /// partitions on all nodes. Accepts replication property
    /// string as a value
    #[clap(long)]
    partition_replication: Option<ReplicationProperty>,

    /// Default log provider kind
    #[clap(long, alias = "log-provider")]
    bifrost_provider: Option<ProviderKind>,

    /// Replication property of bifrost logs if using replicated as log provider
    #[clap(long, required_if_eq("bifrost_provider", "replicated"))]
    log_replication: Option<ReplicationProperty>,

    /// The nodeset size used for replicated log, this is an advanced feature.
    /// It's recommended to leave it unset (defaults to 0)
    #[clap(long, required_if_eq("bifrost_provider", "replicated"))]
    log_default_nodeset_size: Option<u16>,
}

async fn cluster_provision(
    connection: &ConnectionInfo,
    provision_opts: &ProvisionOpts,
) -> anyhow::Result<()> {
    let address = match connection.addresses.len().cmp(&1) {
        Ordering::Greater => {
            let address = &connection.addresses[0];
            c_println!(
                "Cluster provisioning must be performed on a single node. Using {address} for provisioning.",
            );
            address
        }
        Ordering::Equal => &connection.addresses[0],
        Ordering::Less => {
            anyhow::bail!("At least one address must be specified to provision");
        }
    };

    let channel = grpc_channel(address.clone());

    let mut client = NodeCtlSvcClient::new(channel)
        .accept_compressed(CompressionEncoding::Gzip)
        .send_compressed(CompressionEncoding::Gzip);

    let log_provider = provision_opts.bifrost_provider.map(|bifrost_provider| {
        extract_default_provider(
            bifrost_provider,
            provision_opts.log_replication.clone(),
            provision_opts.log_default_nodeset_size.unwrap_or(0),
        )
    });

    let request = ProvisionClusterRequest {
        dry_run: true,
        num_partitions: provision_opts.num_partitions.map(|n| u32::from(n.get())),
        partition_replication: provision_opts.partition_replication.clone().map(Into::into),
        log_provider: log_provider.map(Into::into),
    };

    let response = match client.provision_cluster(request).await {
        Ok(response) => response.into_inner(),
        Err(err) => {
            c_error!(
                "Failed to provision cluster during dry run: {}",
                err.message()
            );
            return Ok(());
        }
    };

    debug_assert!(response.dry_run, "Provision with dry run");
    let cluster_configuration_to_provision = response
        .cluster_configuration
        .expect("Provision response should carry a cluster configuration");

    c_println!(
        "{}",
        cluster_config_string(&cluster_configuration_to_provision)?
    );

    if let Some(default_provider) = &cluster_configuration_to_provision.bifrost_provider {
        let default_provider = ProviderConfiguration::try_from(default_provider.clone())?;

        match default_provider {
            ProviderConfiguration::InMemory | ProviderConfiguration::Local => {
                c_warn!("You are about to provision a cluster with a Bifrost provider that only supports a single node cluster.");
            }
            ProviderConfiguration::Replicated(_) => {
                // nothing to do
            }
        }
    }

    confirm_or_exit("Provision cluster with this configuration?")?;

    let request = ProvisionClusterRequest {
        dry_run: false,
        num_partitions: Some(cluster_configuration_to_provision.num_partitions),
        partition_replication: cluster_configuration_to_provision.partition_replication,
        log_provider: cluster_configuration_to_provision.bifrost_provider,
    };

    match client.provision_cluster(request).await {
        Ok(response) => {
            let response = response.into_inner();
            debug_assert!(!response.dry_run, "Provision w/o dry run");

            c_println!("✅ Cluster has been successfully provisioned.");
        }
        Err(err) => {
            if err.code() == Code::AlreadyExists {
                c_println!("🤷 Cluster has been provisioned by somebody else.");
            } else {
                c_error!("Failed to provision cluster: {}", err.message());
            }
        }
    };

    Ok(())
}

pub fn extract_default_provider(
    bifrost_provider: ProviderKind,
    replication_property: Option<ReplicationProperty>,
    target_nodeset_size: u16,
) -> ProviderConfiguration {
    match bifrost_provider {
        ProviderKind::InMemory => ProviderConfiguration::InMemory,
        ProviderKind::Local => ProviderConfiguration::Local,
        ProviderKind::Replicated => {
            let config = ReplicatedLogletConfig {
                target_nodeset_size,
                replication_property: replication_property.clone().expect("is required"),
            };
            ProviderConfiguration::Replicated(config)
        }
    }
}
