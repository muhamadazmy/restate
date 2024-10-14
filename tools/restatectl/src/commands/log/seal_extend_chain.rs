// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::{NonZeroU32, NonZeroU8};

use anyhow::Context;
use cling::prelude::*;
use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;

use restate_admin::cluster_controller::protobuf::cluster_ctrl_svc_client::ClusterCtrlSvcClient;
use restate_admin::cluster_controller::protobuf::{
    ListLogsRequest, ProviderKind as ProtoProviderKind, SealAndExtendChainRequest,
};
use restate_types::logs::metadata::{Logs, ProviderKind};
use restate_types::logs::LogId;
use restate_types::protobuf::common::Version;
use restate_types::replicated_loglet::{NodeSet, ReplicatedLogletParams, ReplicationProperty};
use restate_types::storage::StorageCodec;
use restate_types::{GenerationalNodeId, PlainNodeId};

use crate::app::ConnectionInfo;
use crate::util::grpc_connect;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[clap()]
#[cling(run = "seal_and_extend_chain")]
pub struct SealAndExtendChainOpts {
    /// LogId/Partition to seal and extend
    #[clap(long)]
    log_id: u32,
    /// Option segment index to seal, default to tail. If provided
    /// it has to be the last segment in the chain
    #[clap(long)]
    segment_index: Option<u32>,
    /// Min metadata version
    #[clap(long, default_value = "1")]
    min_version: NonZeroU32,
    /// Provider kind. One of [local, memory, replicated]
    #[clap(long, default_value = "replicated")]
    provider: ProviderKind,

    /// Replication property
    #[clap(long, required_if_eq("provider", "replicated"))]
    replication_factor: Option<NonZeroU8>,
    /// A comma-separated list of the nodes in the nodeset. e.g. N1,N2,N4 or 1,2,3
    #[clap(long, required_if_eq("provider", "replicated"), value_delimiter=',', num_args = 1..)]
    nodeset: Vec<PlainNodeId>,
    /// The generational node id of the sequencer node, e.g. N1:1
    #[clap(long, required_if_eq("provider", "replicated"))]
    sequencer: Option<GenerationalNodeId>,
}

async fn seal_and_extend_chain(
    connection: &ConnectionInfo,
    opts: &SealAndExtendChainOpts,
) -> anyhow::Result<()> {
    let channel = grpc_connect(connection.cluster_controller.clone())
        .await
        .with_context(|| {
            format!(
                "cannot connect to cluster controller at {}",
                connection.cluster_controller
            )
        })?;

    let mut client =
        ClusterCtrlSvcClient::new(channel).accept_compressed(CompressionEncoding::Gzip);

    let (provider, params) = match opts.provider {
        ProviderKind::Local => (ProtoProviderKind::Local, rand::random::<u64>().to_string()),
        #[cfg(any(test, feature = "memory-loglet"))]
        ProviderKind::InMemory => (ProtoProviderKind::InMemory, String::default()),
        #[cfg(feature = "replicated-loglet")]
        ProviderKind::Replicated => (
            ProtoProviderKind::Replicated,
            replicated_loglet_params(&mut client, opts).await?,
        ),
        #[allow(unreachable_patterns)]
        _ => anyhow::bail!("Provider kind is not supported"),
    };

    client
        .seal_and_extend_chain(SealAndExtendChainRequest {
            log_id: opts.log_id,
            min_version: Some(Version {
                value: opts.min_version.get(),
            }),
            provider: provider as i32,
            segment_index: opts.segment_index,
            params,
        })
        .await?;

    Ok(())
}

async fn replicated_loglet_params(
    client: &mut ClusterCtrlSvcClient<Channel>,
    opts: &SealAndExtendChainOpts,
) -> anyhow::Result<String> {
    let tail_index = match opts.segment_index {
        Some(index) => index,
        None => {
            let mut logs_resposne = client
                .list_logs(ListLogsRequest {})
                .await
                .context("failed to get logs chain")?
                .into_inner();

            let logs = StorageCodec::decode::<Logs, _>(&mut logs_resposne.logs)?;
            let chain = logs
                .chain(&LogId::from(opts.log_id))
                .with_context(|| format!("Unknown log id '{}'", opts.log_id))?;

            chain.tail_index().into()
        }
    };

    // format is, log_id in the higher order u32, and segment_index in the lower
    let loglet_id: u64 =
        (u64::from(opts.log_id) << (size_of::<LogId>() * 8)) + u64::from(tail_index + 1);

    let params = ReplicatedLogletParams {
        loglet_id: loglet_id.into(),
        nodeset: NodeSet::from_iter(opts.nodeset.iter().cloned()),
        replication: ReplicationProperty::new(opts.replication_factor.unwrap()),
        sequencer: opts.sequencer.unwrap(),
        write_set: None,
    };

    params.serialize().map_err(Into::into)
}
