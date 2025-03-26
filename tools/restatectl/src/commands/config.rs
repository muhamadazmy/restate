// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod get;
mod set;

use std::{borrow::Cow, fmt::Write};

use anyhow::Context;
use cling::prelude::*;

use restate_types::{
    logs::metadata::ProviderConfiguration,
    protobuf::cluster::{ClusterConfiguration, PartitionReplicationKind},
};

use crate::util::{write_default_provider, write_leaf};

#[derive(Run, Subcommand, Clone)]
pub enum ConfigOpts {
    /// Print a brief overview of the cluster configuration (nodes, logs, partitions)
    Get(get::ConfigGetOpts),
    /// Set new values for the cluster configuration
    Set(set::ConfigSetOpts),
}

pub fn cluster_config_string(config: &ClusterConfiguration) -> anyhow::Result<String> {
    let mut w = String::default();

    writeln!(w, "⚙️ Cluster Configuration")?;
    write_leaf(
        &mut w,
        0,
        false,
        "Number of partitions",
        config.num_partitions,
    )?;
    let partition_replication_kind: PartitionReplicationKind = config
        .partition_replication_kind
        .try_into()
        .context("invalid partition_replication_kind")?;

    let partition_replication = config
        .partition_replication
        .clone()
        .map(|p| p.replication_property);

    let strategy = match partition_replication_kind {
        PartitionReplicationKind::Everywhere => Cow::Borrowed("*"),
        PartitionReplicationKind::Limit => Cow::Owned(
            partition_replication
                .context("partition_replication is required if replication kind is LIMITED")?,
        ),
        PartitionReplicationKind::Unknown => {
            // backward compatibility with older servers.
            // They will not set the kind field
            partition_replication
                .map(Cow::Owned)
                .unwrap_or(Cow::Borrowed("*"))
        }
    };

    write_leaf(&mut w, 0, false, "Partition replication", strategy)?;

    let provider: ProviderConfiguration = config
        .bifrost_provider
        .clone()
        .unwrap_or_default()
        .try_into()?;

    write_default_provider(&mut w, 0, &provider)?;

    Ok(w)
}
