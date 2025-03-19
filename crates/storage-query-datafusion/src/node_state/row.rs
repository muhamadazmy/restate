// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::schema::NodeStateBuilder;
use restate_types::{PlainNodeId, cluster::cluster_state::NodeState};

const STATE_ALIVE: &str = "ALIVE";
const STATE_DEAD: &str = "DEAD";
const STATE_SUSPECT: &str = "SUSPECT";

#[inline]
pub(crate) fn append_node_row(
    builder: &mut NodeStateBuilder,
    node_id: PlainNodeId,
    node_state: &NodeState,
) {
    let mut row = builder.row();
    row.id(node_id.into());

    match node_state {
        NodeState::Alive(alive) => {
            row.status(STATE_ALIVE);
            row.uptime(alive.uptime.as_secs());
            row.last_seen_ts(alive.last_heartbeat_at.as_u64());
        }
        NodeState::Dead(dead) => {
            row.status(STATE_DEAD);
            if let Some(ts) = dead.last_seen_alive {
                row.last_seen_ts(ts.as_u64());
            }
        }
        NodeState::Suspect(suspect) => {
            row.status(STATE_SUSPECT);
            row.last_attempt_ts(suspect.last_attempt.as_u64());
        }
    }
}
