// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::time::Instant;

use prost_dto::IntoProto;
use serde::{Deserialize, Serialize};

use crate::identifiers::{LeaderEpoch, PartitionId};
use crate::logs::Lsn;
use crate::time::MillisSinceEpoch;
use crate::{GenerationalNodeId, PlainNodeId, Version};

/// A container for health information about every node and partition in the
/// cluster.
#[derive(Debug, Clone, IntoProto)]
#[proto(target = "crate::protobuf::cluster::ClusterState")]
pub struct ClusterState {
    #[into_proto(map = "instant_to_proto")]
    pub last_refreshed: Option<Instant>,
    #[proto(required)]
    pub nodes_config_version: Version,
    #[proto(required)]
    pub partition_table_version: Version,
    #[proto(required)]
    pub logs_metadata_version: Version,
    pub nodes: BTreeMap<PlainNodeId, NodeState>,
}

impl ClusterState {
    pub fn is_reliable(&self) -> bool {
        // todo: make this configurable
        // If the cluster state is older than 10 seconds, then it is not reliable.
        self.last_refreshed
            .map(|last_refreshed| last_refreshed.elapsed().as_secs() < 10)
            .unwrap_or(false)
    }

    pub fn alive_nodes(&self) -> impl Iterator<Item = &AliveNode> {
        self.nodes.values().flat_map(|node| match node {
            NodeState::Alive(alive_node) => Some(alive_node),
            NodeState::Dead(_) | NodeState::Suspect(_) => None,
        })
    }

    pub fn dead_nodes(&self) -> impl Iterator<Item = &PlainNodeId> {
        self.nodes.iter().flat_map(|(node_id, state)| match state {
            NodeState::Alive(_) | NodeState::Suspect(_) => None,
            NodeState::Dead(_) => Some(node_id),
        })
    }

    pub fn empty() -> Self {
        ClusterState {
            last_refreshed: None,
            nodes_config_version: Version::INVALID,
            partition_table_version: Version::INVALID,
            logs_metadata_version: Version::INVALID,
            nodes: BTreeMap::default(),
        }
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn builder() -> ClusterStateBuilder {
        ClusterStateBuilder {
            inner: Self::empty(),
        }
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn into_builder(self) -> ClusterStateBuilder {
        ClusterStateBuilder { inner: self }
    }
}

fn instant_to_proto(t: Instant) -> prost_types::Duration {
    t.elapsed().try_into().unwrap()
}

#[derive(Debug, Clone, IntoProto)]
#[proto(target = "crate::protobuf::cluster::NodeState", oneof = "state")]
pub enum NodeState {
    Alive(AliveNode),
    Dead(DeadNode),
    Suspect(SuspectNode),
}

#[derive(Debug, Clone, IntoProto)]
#[proto(target = "crate::protobuf::cluster::AliveNode")]
pub struct AliveNode {
    #[proto(required)]
    pub last_heartbeat_at: MillisSinceEpoch,
    #[proto(required)]
    pub generational_node_id: GenerationalNodeId,
    pub partitions: BTreeMap<PartitionId, PartitionProcessorStatus>,
}

#[derive(Debug, Clone, IntoProto)]
#[proto(target = "crate::protobuf::cluster::DeadNode")]
pub struct DeadNode {
    pub last_seen_alive: Option<MillisSinceEpoch>,
}

#[derive(Debug, Clone, IntoProto)]
#[proto(target = "crate::protobuf::cluster::SuspectNode")]
/// As the name implies, SuspectNode is both dead and alive
/// until we receive a heartbeat
pub struct SuspectNode {
    #[proto(required)]
    pub generational_node_id: GenerationalNodeId,
    #[proto(required)]
    pub last_attempt: MillisSinceEpoch,
}

#[derive(
    Debug, Clone, Copy, Serialize, Deserialize, Hash, Eq, PartialEq, IntoProto, derive_more::Display,
)]
#[proto(target = "crate::protobuf::cluster::RunMode")]
pub enum RunMode {
    Leader,
    Follower,
}

#[derive(Debug, Clone, Eq, Hash, PartialEq, Serialize, Deserialize, IntoProto)]
#[proto(target = "crate::protobuf::cluster::ReplayStatus")]
pub enum ReplayStatus {
    Starting,
    Active,
    CatchingUp,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, IntoProto)]
#[proto(target = "crate::protobuf::cluster::PartitionProcessorStatus")]
pub struct PartitionProcessorStatus {
    #[proto(required)]
    pub updated_at: MillisSinceEpoch,
    pub planned_mode: RunMode,
    pub effective_mode: RunMode,
    pub last_observed_leader_epoch: Option<LeaderEpoch>,
    pub last_observed_leader_node: Option<GenerationalNodeId>,
    pub last_applied_log_lsn: Option<Lsn>,
    pub last_record_applied_at: Option<MillisSinceEpoch>,
    pub num_skipped_records: u64,
    pub replay_status: ReplayStatus,
    pub last_persisted_log_lsn: Option<Lsn>,
    pub last_archived_log_lsn: Option<Lsn>,
    // Set if replay_status is CatchingUp
    pub target_tail_lsn: Option<Lsn>,
}

impl Default for PartitionProcessorStatus {
    fn default() -> Self {
        Self {
            updated_at: MillisSinceEpoch::now(),
            planned_mode: RunMode::Follower,
            effective_mode: RunMode::Follower,
            last_observed_leader_epoch: None,
            last_observed_leader_node: None,
            last_applied_log_lsn: None,
            last_record_applied_at: None,
            num_skipped_records: 0,
            replay_status: ReplayStatus::Starting,
            last_persisted_log_lsn: None,
            last_archived_log_lsn: None,
            target_tail_lsn: None,
        }
    }
}

impl Hash for PartitionProcessorStatus {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.planned_mode.hash(state);
        self.effective_mode.hash(state);
        if let Some(ref epoch) = self.last_observed_leader_epoch {
            epoch.hash(state);
        }
        if let Some(ref leader_node) = self.last_observed_leader_node {
            leader_node.hash(state);
        }
        self.replay_status.hash(state);
        // NOTE:
        // we intentionally ignoring fields like
        // - updated_at
        // - last_applied_log_lsn
        // - last_record_applied_at
        // - num_skipped_records
        // - last_persisted_log_lsn
        // - target_tail_lsn
        //
        // because we are only interested
        // in attributes that describe the structure
        // of the cluster state and partition processors
        //
        // todo(azmy): review this list because some fields
        // should be propagated when they change
    }
}

impl PartitionProcessorStatus {
    pub fn is_effective_leader(&self) -> bool {
        self.effective_mode == RunMode::Leader
    }

    pub fn new() -> Self {
        Self::default()
    }
}

#[cfg(any(test, feature = "test-util"))]
pub struct ClusterStateBuilder {
    inner: ClusterState,
}

impl ClusterStateBuilder {
    pub fn with_logs_metadata_version(mut self, version: Version) -> Self {
        self.inner.logs_metadata_version = version;
        self
    }

    pub fn with_partition_table_version(mut self, version: Version) -> Self {
        self.inner.partition_table_version = version;
        self
    }

    pub fn with_nodes_config_version(mut self, version: Version) -> Self {
        self.inner.nodes_config_version = version;
        self
    }

    pub fn with_alive_node(mut self, generational_node_id: GenerationalNodeId) -> Self {
        self.inner.nodes.insert(
            generational_node_id.as_plain(),
            NodeState::Alive(AliveNode {
                generational_node_id,
                last_heartbeat_at: MillisSinceEpoch::now(),
                partitions: BTreeMap::default(),
            }),
        );

        self
    }

    pub fn with_dead_node(mut self, plain_node_id: PlainNodeId) -> Self {
        self.inner.nodes.insert(
            plain_node_id,
            NodeState::Dead(DeadNode {
                last_seen_alive: None,
            }),
        );

        self
    }

    pub fn with_partition<M>(
        mut self,
        generational_node_id: GenerationalNodeId,
        partition_id: PartitionId,
        modifier: M,
    ) -> Self
    where
        M: FnOnce(&mut PartitionProcessorStatus),
    {
        let node = self
            .inner
            .nodes
            .get_mut(&generational_node_id.as_plain())
            .expect("node exists");
        let NodeState::Alive(node) = node else {
            panic!("not must be alive");
        };

        let partition = node.partitions.entry(partition_id).or_default();

        modifier(partition);
        self
    }

    pub fn build(self) -> ClusterState {
        self.inner
    }
}
