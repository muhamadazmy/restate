// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{
    collections::{btree_map::Entry, BTreeMap, HashMap, HashSet},
    time::{Duration, Instant},
};

use futures::StreamExt;
use tokio::{
    sync::watch,
    time::{self, MissedTickBehavior},
};
use tracing::{instrument, trace};

use restate_core::{
    cancellation_watcher,
    network::{
        Incoming, MessageRouterBuilder, MessageStream, NetworkSender, Networking, Outgoing,
        TransportConnect,
    },
    worker_api::ProcessorsManagerHandle,
    Metadata, MetadataKind, ShutdownError, TaskCenter, TaskKind,
};
use restate_types::{
    cluster::cluster_state::{AliveNode, ClusterState, DeadNode, NodeState, SuspectNode},
    config::Configuration,
    net::cluster_state::{
        ClusterStateRequest, ClusterStateResponse, GossipPayload, NodeData, NodeHash, NodeRecord,
    },
    time::MillisSinceEpoch,
    GenerationalNodeId, PlainNodeId, Version,
};

const SUSPECT_THRESHOLD_FACTOR: u32 = 2;
const DEAD_THRESHOLD_FACTOR: u32 = 4;

type Peers = HashMap<PlainNodeId, NodeRecord>;

pub struct ClusterStateRefresher<T> {
    my_node_id: GenerationalNodeId,
    gossip_requests: MessageStream<ClusterStateRequest>,
    gossip_responses: MessageStream<ClusterStateResponse>,
    networking: Networking<T>,
    nodes: BTreeMap<PlainNodeId, NodeData>,
    heartbeat_interval: Duration,
    logs_version: Version,
    partition_table_version: Version,
    nodes_config_version: Version,
    cluster_state_watch_tx: watch::Sender<ClusterState>,

    broadcast_set: HashSet<PlainNodeId>,
    // handlers
    processor_manager_handle: Option<ProcessorsManagerHandle>,
}

impl<T> ClusterStateRefresher<T>
where
    T: TransportConnect,
{
    pub(crate) fn new(
        networking: Networking<T>,
        router_builder: &mut MessageRouterBuilder,
    ) -> Self {
        let config = Configuration::pinned();
        ClusterStateRefresher {
            my_node_id: Metadata::with_current(|m| m.my_node_id()),
            gossip_requests: router_builder.subscribe_to_stream(128),
            gossip_responses: router_builder.subscribe_to_stream(128),
            networking,
            nodes: BTreeMap::default(),
            heartbeat_interval: config.common.heartbeat_interval.into(),
            logs_version: Version::MIN,
            partition_table_version: Version::MIN,
            nodes_config_version: Version::MIN,
            cluster_state_watch_tx: watch::Sender::new(ClusterState::empty()),
            broadcast_set: HashSet::default(),
            processor_manager_handle: None,
        }
    }

    pub fn cluster_state_watch(&self) -> watch::Receiver<ClusterState> {
        self.cluster_state_watch_tx.subscribe()
    }

    pub fn with_processor_manager_handle(&mut self, handle: ProcessorsManagerHandle) {
        self.processor_manager_handle = Some(handle);
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        let mut cancelled = std::pin::pin!(cancellation_watcher());
        let mut heartbeat_interval = time::interval(self.heartbeat_interval);
        heartbeat_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let metadata = Metadata::current();
        let mut logs_watcher = metadata.watch(MetadataKind::Logs);
        let mut partition_table_watcher = metadata.watch(MetadataKind::PartitionTable);
        let mut nodes_config_watcher = metadata.watch(MetadataKind::NodesConfiguration);

        loop {
            tokio::select! {
                _ = &mut cancelled => {
                    break;
                }
                _ = heartbeat_interval.tick() => {
                    self.on_heartbeat(false).await?;
                }
                _ = logs_watcher.changed() => {
                    self.logs_version = *logs_watcher.borrow();
                    if self.is_latest_version() {
                        self.on_heartbeat(true).await?;
                    }
                }
                _ = partition_table_watcher.changed() => {
                    self.partition_table_version= *partition_table_watcher.borrow();
                    if self.is_latest_version() {
                        self.on_heartbeat(true).await?;
                    }
                }
                _ = nodes_config_watcher.changed() => {
                    self.nodes_config_version = *nodes_config_watcher.borrow();
                    if self.is_latest_version() {
                        self.on_heartbeat(true).await?;
                    }
                }
                Some(request) = self.gossip_requests.next() => {
                    self.on_gossip_request(request).await?;
                }
                Some(responses) = self.gossip_responses.next() => {
                    self.on_gossip_response(responses).await?;
                }

            }
        }

        Ok(())
    }

    /// checks if metadata is at latest "seen" versions of
    /// logs, partition table, and nodes_config
    fn is_latest_version(&self) -> bool {
        let metadata = Metadata::current();
        metadata.logs_version() >= self.logs_version
            && metadata.partition_table_version() >= self.partition_table_version
            && metadata.nodes_config_version() >= self.nodes_config_version
    }

    #[instrument(level="debug", parent=None, skip(self), fields(
        logs_version=%self.logs_version,
        partition_table_version=%self.partition_table_version,
        nodes_config_version=%self.nodes_config_version,
    ))]
    async fn on_heartbeat(&mut self, all_nodes: bool) -> Result<(), ShutdownError> {
        // notify all watcher about the current known state
        // of the cluster.
        self.cluster_state_watch_tx.send_if_modified(|_| true);

        let nodes = Metadata::with_current(|m| m.nodes_config_ref());
        for (node_id, _) in nodes.iter() {
            if node_id == self.my_node_id.as_plain() {
                continue;
            }

            let local_node_state = self.nodes.get(&node_id);
            let last_seen_since = local_node_state.map(|s| s.timestamp.elapsed());
            match last_seen_since {
                None => {
                    // never seen!
                    // we try to ping it
                }
                Some(since) => {
                    if !all_nodes && since < self.heartbeat_interval {
                        // we have recent state of the node and we don't have
                        // to ping all nodes
                        continue;
                    }
                }
            }

            self.broadcast_set.insert(node_id);
        }

        trace!(
            "Gossip request to {} nodes: {:?}",
            self.broadcast_set.len(),
            self.broadcast_set
        );

        let request = ClusterStateRequest {
            payload: GossipPayload {
                data: self.my_state().await?,
                cluster: self
                    .nodes
                    .iter()
                    .map(|(id, state)| (*id, NodeHash::from(state).into()))
                    .collect(),
            },
        };

        for node in self.broadcast_set.drain() {
            let msg = Outgoing::new(node, request.clone());

            let networking = self.networking.clone();
            let _ =
                TaskCenter::spawn_child(TaskKind::Disposable, "send-gossip-request", async move {
                    // ignore send errors
                    let _ = networking.send(msg).await;
                    Ok(())
                });
        }

        Ok(())
    }

    async fn my_state(&self) -> Result<NodeData, ShutdownError> {
        Ok(NodeData {
            timestamp: MillisSinceEpoch::now(),
            generational_node_id: self.my_node_id,
            partitions: if let Some(ref handle) = self.processor_manager_handle {
                handle.get_state().await?
            } else {
                BTreeMap::default()
            },
        })
    }

    fn update_cluster_state(&self) {
        let nodes = Metadata::with_current(|m| m.nodes_config_ref());

        let suspect_duration = self.heartbeat_interval * SUSPECT_THRESHOLD_FACTOR;
        let dead_duration = self.heartbeat_interval * DEAD_THRESHOLD_FACTOR;

        let cluster_state = ClusterState {
            last_refreshed: Some(Instant::now()),
            logs_metadata_version: self.logs_version,
            nodes_config_version: self.nodes_config_version,
            partition_table_version: self.partition_table_version,
            nodes: nodes
                .iter()
                .map(|(node_id, _)| {
                    let node_data = self.nodes.get(&node_id);
                    let node_state = match node_data {
                        None => NodeState::Dead(DeadNode {
                            last_seen_alive: None,
                        }),
                        Some(data) => {
                            let elapsed = data.timestamp.elapsed();
                            if elapsed < suspect_duration {
                                NodeState::Alive(AliveNode {
                                    generational_node_id: data.generational_node_id,
                                    last_heartbeat_at: data.timestamp,
                                    partitions: data.partitions.clone(),
                                })
                            } else if elapsed >= suspect_duration && elapsed < dead_duration {
                                NodeState::Suspect(SuspectNode {
                                    generational_node_id: data.generational_node_id,
                                    last_attempt: data.timestamp,
                                })
                            } else {
                                NodeState::Dead(DeadNode {
                                    last_seen_alive: Some(data.timestamp),
                                })
                            }
                        }
                    };

                    (node_id, node_state)
                })
                .collect(),
        };

        // the cluster state is always sent silently so we don't trigger the update
        // on each ping request/response received from peers.
        // Instead, we just make sure the watcher has updated view, and then trigger
        // `changed` on heartbeat.
        self.cluster_state_watch_tx.send_if_modified(|old| {
            *old = cluster_state;
            false
        });
    }

    /// update our local view with received state
    async fn merge_views<P: Into<PlainNodeId>>(
        &mut self,
        sender: P,
        payload: GossipPayload,
    ) -> Result<Peers, ShutdownError> {
        let peer = sender.into();

        let GossipPayload {
            data: mut peer_state,
            mut cluster,
        } = payload;

        let mut diff = Peers::default();
        peer_state.timestamp = MillisSinceEpoch::now();
        self.nodes.insert(peer, peer_state);

        // full snapshot of the peer view of cluster state.
        let nodes_config = Metadata::with_current(|m| m.nodes_config_ref());

        for (node_id, _) in nodes_config.iter() {
            if node_id == self.my_node_id.as_plain() {
                continue;
            }

            let local_node_view = self.nodes.entry(node_id);

            let Some(peer_node_view) = cluster.remove(&node_id) else {
                // peer does not know about this node, do we know about it?
                if let Entry::Occupied(local_view) = local_node_view {
                    diff.insert(node_id, local_view.get().clone().into());
                }

                continue;
            };

            match (local_node_view, peer_node_view) {
                (Entry::Vacant(entry), NodeRecord::Data(data)) => {
                    entry.insert(data);
                }
                (Entry::Vacant(_), NodeRecord::Hash(_)) => {
                    self.broadcast_set.insert(node_id);
                }
                (Entry::Occupied(mut local), NodeRecord::Data(remote)) => {
                    let local = local.get_mut();
                    match (
                        remote.timestamp > local.timestamp,
                        remote.hashed() == local.hashed(),
                    ) {
                        (true, _) => {
                            *local = remote;
                        }
                        (false, true) => {
                            diff.insert(node_id, NodeRecord::Hash(local.into()));
                        }
                        (false, false) => {
                            diff.insert(node_id, NodeRecord::Data(local.clone()));
                        }
                    }
                }
                (Entry::Occupied(mut local), NodeRecord::Hash(remote)) => {
                    let local = local.get_mut();

                    match (
                        remote.timestamp > local.timestamp,
                        remote.hash == local.hashed(),
                    ) {
                        (true, true) => {
                            // only update the local timestamp
                            local.timestamp = remote.timestamp;
                        }
                        (true, false) => {
                            // we need to update our view.
                            self.broadcast_set.insert(node_id);
                        }
                        (false, true) => {
                            // local is more recent but same data
                            diff.insert(node_id, NodeRecord::Hash(local.into()));
                        }
                        (false, false) => {
                            // we have a more recent view
                            diff.insert(node_id, NodeRecord::Data(local.clone()));
                        }
                    }
                }
            }
        }

        self.update_cluster_state();

        trace!(
            "Cluster state updated. Learned about {}/{} nodes",
            self.nodes.len() - diff.len(),
            self.nodes.len()
        );

        Ok(diff)
    }

    #[instrument(level="debug", parent=None, skip_all, fields(
        peer=msg.peer().to_string()
    ))]
    async fn on_gossip_response(
        &mut self,
        mut msg: Incoming<ClusterStateResponse>,
    ) -> Result<(), ShutdownError> {
        msg.follow_from_sender();

        trace!("Gossip response");

        self.merge_views(msg.peer(), msg.into_body().payload)
            .await?;

        Ok(())
    }

    #[instrument(level="debug", parent=None, skip_all, fields(
        peer=msg.peer().to_string()
    ))]
    async fn on_gossip_request(
        &mut self,
        mut msg: Incoming<ClusterStateRequest>,
    ) -> Result<(), ShutdownError> {
        msg.follow_from_sender();

        trace!("Gossip request");
        let peer = msg.peer();

        let reciprocal = msg.create_reciprocal();

        let body = msg.into_body();
        let diff = self.merge_views(peer, body.payload).await?;

        let msg = ClusterStateResponse {
            payload: GossipPayload {
                data: self.my_state().await?,
                cluster: diff,
            },
        };
        let outgoing = reciprocal.prepare(msg);

        let _ = outgoing.try_send();
        Ok(())
    }
}
