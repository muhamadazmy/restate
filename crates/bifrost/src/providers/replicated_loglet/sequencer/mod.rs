// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod append;
mod node;

use restate_core::{network::NetworkError, ShutdownError};
use restate_types::{
    logs::LogletOffset, nodes_config::NodesConfigError, replicated_loglet::ReplicatedLogletId,
    GenerationalNodeId,
};
use tokio::sync::watch;

/// A sharable part of the sequencer state. This is shared with node workers
#[derive(Debug)]
pub(crate) struct SequencerGlobalState {
    node_id: GenerationalNodeId,
    loglet_id: ReplicatedLogletId,
    committed_tail: watch::Sender<LogletOffset>,
}

impl SequencerGlobalState {
    pub fn node_id(&self) -> &GenerationalNodeId {
        &self.node_id
    }

    pub fn loglet_id(&self) -> &ReplicatedLogletId {
        &self.loglet_id
    }

    pub fn committed_tail(&self) -> LogletOffset {
        *self.committed_tail.borrow()
    }

    /// update committed tail if and only if new_tail is greater than
    /// current global committed tail
    pub fn maybe_update_committed_tail(&self, new_tail: LogletOffset) {
        self.committed_tail.send_if_modified(|t| {
            if new_tail > *t {
                *t = new_tail;
                true
            } else {
                false
            }
        });
    }

    /// wait for global tail to be greater than or equal new_tail
    pub async fn wait_for_committed_tail(&self, new_tail: LogletOffset) -> Option<LogletOffset> {
        let mut subscriber = self.committed_tail.subscribe();
        subscriber
            .wait_for(|f| *f >= new_tail)
            .await
            .map(|t| *t)
            .ok()
    }
}

#[derive(thiserror::Error, Debug)]
enum Error {
    #[error("invalid node-set configuration")]
    InvalidNodeSet,

    #[error(transparent)]
    Network(#[from] NetworkError),
    #[error(transparent)]
    NodeConfig(#[from] NodesConfigError),
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
}
