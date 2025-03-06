// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Context;
use clap::Parser;
use cling::{Collect, Run};
use itertools::Itertools;

use restate_cli_util::c_println;
use restate_core::protobuf::metadata_proxy_svc::MetadataStoreProxy;
use restate_metadata_server::MetadataStoreClient;
use restate_types::PlainNodeId;
use restate_types::config::MetadataClientOptions;
use restate_types::logs::metadata::Logs;
use restate_types::metadata_store::keys::NODES_CONFIG_KEY;
use restate_types::nodes_config::NodesConfiguration;

use crate::commands::node::disable_node_checker::DisableNodeChecker;
use crate::connection::{ConnectionInfo, NodeOperationError};

#[derive(Run, Parser, Collect, Clone, Debug)]
#[clap(alias = "rm")]
#[cling(run = "remove_nodes")]
pub struct RemoveNodesOpts {
    /// The node/s to remove from the cluster. Specify multiple nodes as a comma-separated list or
    /// specify the option multiple times.
    #[arg(long, required = true, visible_alias = "node", value_delimiter = ',')]
    nodes: Vec<PlainNodeId>,
}

pub async fn remove_nodes(
    connection: &ConnectionInfo,
    opts: &RemoveNodesOpts,
) -> anyhow::Result<()> {
    let logs = connection.get_logs().await?;

    let backoff_policy = &MetadataClientOptions::default().backoff_policy;

    connection
        .try_each(None, |channel| async {
            let metadata_store_proxy = MetadataStoreProxy::new(channel);
            let metadata_store_client =
                MetadataStoreClient::new(metadata_store_proxy, Some(backoff_policy.clone()));

            update_nodes_configuration(&metadata_store_client, &logs, opts).await
        })
        .await?;

    c_println!(
        "Successfully removed nodes [{}]",
        opts.nodes.iter().join(",")
    );

    Ok(())
}

async fn update_nodes_configuration(
    client: &MetadataStoreClient,
    logs: &Logs,
    opts: &RemoveNodesOpts,
) -> Result<(), NodeOperationError> {
    client
        .read_modify_write::<_, _, anyhow::Error>(
            NODES_CONFIG_KEY.clone(),
            |nodes_configuration: Option<NodesConfiguration>| {
                let mut nodes_configuration =
                    nodes_configuration.context("Missing nodes configuration")?;

                let disable_node_checker = DisableNodeChecker::new(&nodes_configuration, logs);

                for node_id in &opts.nodes {
                    disable_node_checker
                        .safe_to_disable_node(*node_id)
                        .context("It is not safe to disable node {node_id}")?;
                }

                for node_id in &opts.nodes {
                    nodes_configuration.remove_node_unchecked(*node_id);
                }

                nodes_configuration.increment_version();

                Ok(nodes_configuration)
            },
        )
        .await
        .map(|_| ())
        .map_err(Into::into)
}
