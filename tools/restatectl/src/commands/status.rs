// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Display;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use cling::{Collect, Run};
use enumset::EnumSet;
use itertools::Itertools;
use tonic::codec::CompressionEncoding;

use restate_admin::cluster_controller::protobuf::cluster_ctrl_svc_client::ClusterCtrlSvcClient;
use restate_admin::cluster_controller::protobuf::ClusterStateRequest;
use restate_cli_util::_comfy_table::{Cell, Color, Row, Table};
use restate_cli_util::c_println;
use restate_cli_util::ui::console::StyledTable;
use restate_types::logs::metadata::Logs;
use restate_types::nodes_config::Role;
use restate_types::protobuf::cluster::node_state::State;
use restate_types::protobuf::cluster::{AliveNode, RunMode};
use restate_types::{GenerationalNodeId, NodeId, PlainNodeId};

use crate::commands::log::list_logs::{list_logs, ListLogsOpts};
use crate::commands::metadata_server::status::list_metadata_servers;
use crate::commands::node::list_nodes::{list_nodes, ListNodesOpts};
use crate::commands::partition::list::{list_partitions, ListPartitionsOpts};
use crate::connection::ConnectionInfo;

use super::log::deserialize_replicated_log_params;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "cluster_status")]
pub struct ClusterStatusOpts {
    /// Display additional status information
    #[arg(long)]
    extra: bool,
}

async fn cluster_status(
    connection: &ConnectionInfo,
    status_opts: &ClusterStatusOpts,
) -> anyhow::Result<()> {
    if !status_opts.extra {
        compact_cluster_status(connection).await?;

        return Ok(());
    }

    list_nodes(connection, &ListNodesOpts { extra: false }).await?;
    c_println!();

    list_logs(connection, &ListLogsOpts {}).await?;
    c_println!();

    list_partitions(connection, &ListPartitionsOpts::default()).await?;
    c_println!();

    list_metadata_servers(connection).await?;

    Ok(())
}

async fn compact_cluster_status(connection: &ConnectionInfo) -> anyhow::Result<()> {
    let nodes_config = connection.get_nodes_configuration().await?;
    let logs = connection.get_logs().await?;

    let cluster_state = connection
        .try_each(Some(Role::Admin), |channel| async {
            let mut client = ClusterCtrlSvcClient::new(channel)
                .accept_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Gzip);

            client
                .get_cluster_state(ClusterStateRequest::default())
                .await
        })
        .await?
        .into_inner()
        .cluster_state
        .context("no cluster state returned")?;

    let mut table = Table::new_styled();
    table.set_styled_header(NodeRow::header());
    for (node_id, node_state) in &cluster_state.nodes {
        let node_id = PlainNodeId::new(*node_id);
        let Ok(node) = nodes_config.find_node_by_id(node_id) else {
            // node was deleted or unknown
            continue;
        };

        let node_state = node_state.state.as_ref().context("missing node state")?;
        let mut row = NodeRow::default();
        row.with_name(node.name.clone()).with_roles(&node.roles);

        match node_state {
            State::Alive(alive) => {
                // test
                row.with_id(
                    GenerationalNodeId::from(
                        alive.generational_node_id.context("node id is missing")?,
                    ),
                    Color::Green,
                )
                .with_uptime(Duration::from_secs(alive.age_s));

                aggregate_status(&mut row, alive, &logs).await?;
            }
            State::Dead(_dead) => {
                row.with_id(node_id, Color::Red);
            }
            State::Suspect(suspect) => {
                row.with_id(
                    suspect.generational_node_id.context("node id is missing")?,
                    Color::Yellow,
                );
            }
        };

        table.add_row(row);
    }

    c_println!("Node Configuration ({})", nodes_config.version());
    c_println!("{}", table);
    Ok(())
}

#[derive(Default)]
struct PartitionCounter {
    leaders: u16,
    followers: u16,
    upgrading: u16,
    downgrading: u16,
}

impl Display for PartitionCounter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "L:{}", self.leaders)?;
        if self.upgrading > 0 {
            write!(f, "+{}", self.upgrading)?;
        }
        write!(f, " F:{}", self.followers)?;
        if self.downgrading > 0 {
            write!(f, "+{}", self.downgrading)?;
        }

        Ok(())
    }
}

struct LogsCounter {
    sequencers: usize,
    nodesets: usize,
    optimal: bool,
}

impl Default for LogsCounter {
    fn default() -> Self {
        Self {
            sequencers: 0,
            nodesets: 0,
            optimal: true,
        }
    }
}

impl Display for LogsCounter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.nodesets)?;
        if self.sequencers != 0 {
            write!(f, " (S:{})", self.sequencers)?;
        }

        Ok(())
    }
}

async fn aggregate_status(row: &mut NodeRow, node: &AliveNode, logs: &Logs) -> anyhow::Result<()> {
    let node_id: GenerationalNodeId = node
        .generational_node_id
        .context("node id is missing")?
        .into();

    let counter = node.partitions.values().fold(
        PartitionCounter::default(),
        |mut counter, partition_status| {
            let effective =
                RunMode::try_from(partition_status.effective_mode).expect("valid effective mode");
            let planned =
                RunMode::try_from(partition_status.planned_mode).expect("valid planned mode");

            match (effective, planned) {
                (RunMode::Leader, RunMode::Leader) => counter.leaders += 1,
                (RunMode::Follower, RunMode::Follower) => counter.followers += 1,
                (RunMode::Leader, RunMode::Follower) => counter.downgrading += 1,
                (RunMode::Follower, RunMode::Leader) => counter.upgrading += 1,
                (_, _) => {
                    // unknown state!
                }
            };

            counter
        },
    );

    row.with_partitions(counter);

    let mut counter = LogsCounter::default();

    for (log_id, chain) in logs.iter() {
        let tail = chain.tail();
        let Some(replicated_loglet_params) = deserialize_replicated_log_params(&tail) else {
            continue;
        };

        if replicated_loglet_params.nodeset.contains(node_id) {
            counter.nodesets += 1;
        }

        if replicated_loglet_params.sequencer != node_id {
            // sequencer fot this log is not running here
            continue;
        }

        // check if we also running the partition
        counter.sequencers += 1;
        let Some(partition_status) = node.partitions.get(&u32::from(*log_id)) else {
            // partition is not running here
            counter.optimal = false;
            continue;
        };

        let effective =
            RunMode::try_from(partition_status.effective_mode).expect("valid effective mode");
        let planned = RunMode::try_from(partition_status.planned_mode).expect("valid planned mode");

        // or partition is not (or not becoming) the leader
        if effective != RunMode::Leader && planned != RunMode::Leader {
            counter.optimal = false;
        }
    }
    row.with_logs(counter);

    Ok(())
}

#[derive(Default)]
struct NodeRow {
    id: Option<Cell>,
    name: Option<Cell>,
    uptime: Option<Cell>,
    roles: Option<Cell>,
    partitions: Option<Cell>,
    logs: Option<Cell>,
}

impl NodeRow {
    fn header() -> Vec<&'static str> {
        vec!["NODE-ID", "NAME", "UPTIME", "ROLES", "PARTITIONS", "LOGS"]
    }

    fn with_id<I: Into<NodeId>>(&mut self, node_id: I, color: Color) -> &mut Self {
        self.id = Some(Cell::new(node_id.into()).fg(color));
        self
    }

    fn with_uptime(&mut self, uptime: Duration) -> &mut Self {
        let uptime = humantime::Duration::from(uptime);
        self.uptime = Some(Cell::new(format!("{}", uptime)));
        self
    }

    fn with_name(&mut self, name: String) -> &mut Self {
        self.name = Some(Cell::new(name));
        self
    }

    fn with_roles(&mut self, roles: &EnumSet<Role>) -> &mut Self {
        let mask: EnumSet<Role> =
            Role::Admin | Role::LogServer | Role::MetadataServer | Role::Worker;

        let mut buf = ['-'; 4];
        for (index, role) in mask.iter().sorted_by_key(|r| r.to_string()).enumerate() {
            if roles.contains(role) {
                buf[index] = role.to_string().to_uppercase().chars().next().unwrap();
            }
        }

        self.roles = Some(Cell::new(String::from_iter(&buf)));
        self
    }

    fn with_partitions(&mut self, counter: PartitionCounter) -> &mut Self {
        self.partitions = Some(Cell::new(counter));
        self
    }

    fn with_logs(&mut self, counter: LogsCounter) -> &mut Self {
        let mut cell = Cell::new(&counter).fg(Color::Yellow);
        if counter.optimal {
            cell = cell.fg(Color::Green);
        }
        self.logs = Some(cell);
        self
    }
}

// helper macro to unwrap cells
macro_rules! unwrap {
    ($cell:expr) => {
        $cell.unwrap_or_else(|| Cell::new(""))
    };
    ($cell:expr, $default:expr) => {
        $cell.unwrap_or_else(|| Cell::new($default))
    };
    ($cell:expr, $default:expr => $color:expr) => {
        $cell.unwrap_or_else(|| Cell::new($default).fg($color))
    };
}

impl From<NodeRow> for Row {
    fn from(value: NodeRow) -> Self {
        let row = vec![
            value.id.expect("must be set"),
            unwrap!(value.name),
            unwrap!(value.uptime, "offline" => Color::Red),
            unwrap!(value.roles),
            unwrap!(value.partitions),
            unwrap!(value.logs),
        ];
        row.into()
    }
}
