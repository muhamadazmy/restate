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
use cling::prelude::*;
use tonic::codec::CompressionEncoding;
use tracing::error;

use restate_admin::cluster_controller::protobuf::cluster_ctrl_svc_client::ClusterCtrlSvcClient;
use restate_admin::cluster_controller::protobuf::CreatePartitionSnapshotRequest;
use restate_cli_util::c_println;
use restate_types::nodes_config::Role;

use crate::connection::ConnectionInfo;
use crate::util::RangeParam;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[clap(visible_alias = "create")]
#[cling(run = "create_snapshot")]
pub struct CreateSnapshotOpts {
    /// The partition id or range to snapshot, e.g. "0", "1-4"
    #[arg(required = true)]
    partition_id: Vec<RangeParam>,
}

async fn create_snapshot(
    connection: &ConnectionInfo,
    opts: &CreateSnapshotOpts,
) -> anyhow::Result<()> {
    for partition_id in opts.partition_id.iter().flatten() {
        // make sure partition_id fits in a u16
        if let Err(err) = inner_create_snapshot(connection, partition_id).await {
            error!("Failed to create snapshot for partition {partition_id}: {err}");
        }
    }
    Ok(())
}

async fn inner_create_snapshot(
    connection: &ConnectionInfo,
    partition_id: u32,
) -> anyhow::Result<()> {
    let request = CreatePartitionSnapshotRequest { partition_id };

    let response = connection
        .try_each(Some(Role::Admin), |channel| async {
            let mut client = ClusterCtrlSvcClient::new(channel)
                .accept_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Gzip);

            client.create_partition_snapshot(request).await
        })
        .await
        .context("Failed to request snapshot")?
        .into_inner();

    c_println!(
        "Snapshot created for partition {partition_id}: {}",
        response.snapshot_id
    );

    Ok(())
}
