// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{collections::BTreeMap, ops::Deref, sync::Arc};

use super::Error;
use restate_core::{
    network::{ConnectionManager, ConnectionSender},
    Metadata,
};
use restate_types::{
    logs::{LogletOffset, SequenceNumber},
    replicated_loglet::NodeSet,
    PlainNodeId,
};
use tokio::sync::{watch, Mutex};

#[derive(Debug, Clone)]
/// sharable in memory log server state.
pub struct LogServerState {
    sealed: watch::Sender<bool>,
    local_tail: watch::Sender<LogletOffset>,
}

impl Default for LogServerState {
    fn default() -> Self {
        Self {
            sealed: watch::Sender::new(false),
            local_tail: watch::Sender::new(LogletOffset::OLDEST),
        }
    }
}

impl LogServerState {
    /// check if log server is sealed or not
    pub fn is_sealed(&self) -> bool {
        *self.sealed.borrow()
    }

    /// seal log server only marks log server as sealed.
    /// it also notify all waiters on seal
    pub fn seal(&self) {
        self.sealed.send_if_modified(|s| {
            if *s != true {
                *s = true;
                true
            } else {
                false
            }
        });
    }

    /// get current local tail
    pub fn local_tail(&self) -> LogletOffset {
        *self.local_tail.borrow()
    }

    /// update server local tail if and only if new tail is newer
    /// that last known tail value
    pub fn maybe_update_local_tail(&self, new_tail: LogletOffset) {
        self.local_tail.send_if_modified(|m| {
            if new_tail > *m {
                *m = new_tail;
                true
            } else {
                false
            }
        });
    }

    /// wait for tail to be at this value or higher
    pub async fn wait_for_tail(&self, value: LogletOffset) -> Option<LogletOffset> {
        let mut receiver = self.local_tail.subscribe();
        receiver.wait_for(|v| *v >= value).await.map(|f| *f).ok()
    }

    /// wait for seal
    pub async fn wait_for_seal(&self) -> Option<bool> {
        let mut receiver = self.sealed.subscribe();
        receiver.wait_for(|v| *v).await.map(|f| *f).ok()
    }
}

/// LogServer instance
#[derive(Clone)]
pub struct LogServer {
    node: PlainNodeId,
    state: LogServerState,
    sender: ConnectionSender,
}

impl LogServer {
    pub fn node(&self) -> &PlainNodeId {
        &self.node
    }

    pub fn state(&self) -> &LogServerState {
        &self.state
    }

    pub fn sender(&self) -> ConnectionSender {
        self.sender.clone()
    }
}

#[derive(Default)]
struct LogServerLock(Mutex<Option<LogServer>>);

/// LogServerManager maintains a set of log servers that provided via the
/// [`NodeSet`].
///
/// The manager makes sure there is only one active connection per server.
/// It's up to the user of the client to do [`LogServerManager::renew`] if needed
pub(crate) struct LogServerManager {
    servers: Arc<BTreeMap<PlainNodeId, LogServerLock>>,
    node_set: NodeSet,
    metadata: Metadata,
    connection_manager: ConnectionManager,
}

impl Clone for LogServerManager {
    fn clone(&self) -> Self {
        Self {
            servers: Arc::clone(&self.servers),
            node_set: self.node_set.clone(),
            metadata: self.metadata.clone(),
            connection_manager: self.connection_manager.clone(),
        }
    }
}

impl LogServerManager {
    /// creates the node set and start the appenders
    pub fn new(
        metadata: Metadata,
        connection_manager: ConnectionManager,
        node_set: NodeSet,
    ) -> Result<Self, super::Error> {
        let mut servers = BTreeMap::default();
        for node_id in node_set.iter() {
            servers.insert(*node_id, LogServerLock::default());
        }

        Ok(Self {
            servers: Arc::new(servers),
            node_set,
            metadata,
            connection_manager,
        })
    }

    async fn connect(&self, id: PlainNodeId) -> Result<ConnectionSender, Error> {
        let conf = self.metadata.nodes_config_ref();
        let node = conf.find_node_by_id(id)?;
        let connection = self
            .connection_manager
            .get_node_sender(node.current_generation)
            .await?;

        Ok(connection)
    }

    /// gets a log-server instance. On fist time it will initialize a new connection
    /// to log server. It will make sure all following get call will hold the same
    /// connection.
    ///
    /// it's up to the client to call [`Self::renew`] if the connection it holds
    /// is closed
    pub async fn get(&self, id: PlainNodeId) -> Result<LogServer, Error> {
        let server = self.servers.get(&id).ok_or(Error::InvalidNodeSet)?;

        let mut guard = server.0.lock().await;

        if let Some(current) = guard.deref() {
            return Ok(current.clone());
        }

        // initialize a new instance
        let server = LogServer {
            node: id,
            state: LogServerState::default(),
            sender: self.connect(id).await?,
        };

        // we need to update initialize it
        *guard = Some(server.clone());

        Ok(server)
    }

    /// renew makes sure server connection is renewed if and only if
    /// the provided server holds an outdated connection. Otherwise
    /// the latest connection associated with this server is used.
    ///
    /// It's up the holder of the log server instance to retry to renew
    /// if that connection is not valid.
    ///
    /// It also grantees that concurrent call to renew on the same server instance
    /// will only renew the connection once for all callers
    pub async fn renew(&self, server: &mut LogServer) -> Result<(), Error> {
        if !server.sender.is_closed() {
            // no need to renew!
            return Ok(());
        }

        // this key must already be in the map
        let current = self
            .servers
            .get(&server.node)
            .ok_or(Error::InvalidNodeSet)?;

        let mut guard = current.0.lock().await;

        // if you calling renew then the LogServer has already been initialized
        let inner = guard.as_mut().expect("initialized log server instance");

        if inner.sender != server.sender {
            // someone else has already renewed the connection
            server.sender = inner.sender.clone();
            return Ok(());
        }

        let sender = self.connect(server.node).await?;
        inner.sender = sender.clone();
        server.sender = sender.clone();

        Ok(())
    }
}
