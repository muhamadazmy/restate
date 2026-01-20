// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::RangeInclusive;

use restate_storage_api::fsm_table::StoredReplicaSetState;
use restate_types::identifiers::{LeaderEpoch, PartitionId, PartitionKey};
use restate_types::logs::{Keys, Lsn, SequenceNumber};
use restate_types::partitions::PartitionConfiguration;
use restate_types::partitions::state::{MemberState, ReplicaSetState};
use restate_types::replication::NodeSet;
use restate_types::schema::Schema;
use restate_types::time::MillisSinceEpoch;
use restate_types::{GenerationalNodeId, SemanticRestateVersion, Version, Versioned};

/// Announcing a new leader. This message can be written by any component to make the specified
/// partition processor the leader.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AnnounceLeader {
    /// Sender of the announce leader message.
    ///
    /// This became non-optional in v1.5. Noting that it has always been set in previous versions,
    /// it's safe to assume that it's always set.
    pub node_id: GenerationalNodeId,
    pub leader_epoch: LeaderEpoch,
    pub partition_key_range: RangeInclusive<PartitionKey>,
    /// Current replica set configuration at the time of the announcement.
    /// This field is optional for backward compatibility with older versions.
    /// *Since v1.6*
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub current_config: Option<ReplicaSetConfiguration>,
    /// Next replica set configuration (if a reconfiguration is in progress).
    /// This field is optional for backward compatibility with older versions.
    /// *Since v1.6*
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub next_config: Option<ReplicaSetConfiguration>,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ReplicaSetConfiguration {
    pub version: Version,
    pub replica_set: NodeSet,
    pub modified_at: MillisSinceEpoch,
}

impl ReplicaSetConfiguration {
    pub fn to_replica_set_state(&self) -> ReplicaSetState {
        let members = self
            .replica_set
            .iter()
            .map(|node_id| MemberState {
                node_id: *node_id,
                durable_lsn: Lsn::INVALID,
            })
            .collect();

        ReplicaSetState {
            version: self.version,
            members,
        }
    }
}

impl From<PartitionConfiguration> for ReplicaSetConfiguration {
    fn from(value: PartitionConfiguration) -> Self {
        Self {
            version: value.version(),
            replica_set: value.replica_set().clone(),
            modified_at: value.modified_at(),
        }
    }
}

impl From<ReplicaSetConfiguration> for StoredReplicaSetState {
    fn from(value: ReplicaSetConfiguration) -> Self {
        Self {
            modified_at: value.modified_at,
            replica_set: value.to_replica_set_state(),
        }
    }
}

/// A version barrier to fence off state machine changes that require a certain minimum
/// version of restate server.
///
/// Readers before v1.4.0 will crash when reading this command. For v1.4.0+, the barrier defines the
/// minimum version of restate server that can progress after this command. It also updates the FSM
/// in case command has been trimmed.
#[derive(Debug, Clone, PartialEq, Eq, bilrost::Message)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct VersionBarrier {
    /// The minimum version required (inclusive) to progress after this barrier.
    pub version: SemanticRestateVersion,
    /// A human-readable reason for why this barrier exists.
    pub human_reason: Option<String>,
    pub partition_key_range: Keys,
}

/// Updates the `PARTITION_DURABILITY` FSM variable to the given value. Note that durability
/// only applies to partitions with the same `partition_id`. At replay time, the partition will
/// ignore updates that are not targeted to its own ID.
///
/// NOTE: The durability point is monotonically increasing.
///
/// Since v1.4.2.
#[derive(Debug, Clone, PartialEq, Eq, bilrost::Message)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct PartitionDurability {
    pub partition_id: PartitionId,
    /// The partition has applied this LSN durably to the replica-set and/or has been
    /// persisted in a snapshot in the snapshot repository.
    pub durable_point: Lsn,
    /// Timestamp which the durability point was updated
    pub modification_time: MillisSinceEpoch,
}

/// Consistently store schema across partition replicas.
///
/// Since v1.6.0.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct UpsertSchema {
    pub partition_key_range: Keys,
    pub schema: Schema,
}
