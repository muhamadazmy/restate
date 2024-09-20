use std::sync::Arc;

use futures::StreamExt;
use tokio::task::JoinSet;

use restate_core::{
    network::{rpc_router::ResponseTracker, Incoming, NetworkError, Outgoing},
    Metadata,
};
use restate_types::{
    logs::{LogletOffset, Record, SequenceNumber, TailState},
    net::log_server::{Status, Store, StoreFlags, Stored},
    replicated_loglet::NodeSet,
};

use super::{
    node::{RemoteLogServer, RemoteLogServerManager},
    SequencerGlobalState,
};
use crate::providers::replicated_loglet::replication::spread_selector::SpreadSelector;

enum AppenderState {
    Wave {
        replicated: NodeSet,
        gray_list: NodeSet,
    },
    Done,
}

/// Appender makes sure a batch of records will run to completion
pub(crate) struct Appender {
    global: Arc<SequencerGlobalState>,
    log_server_manager: RemoteLogServerManager,
    response_tracker: ResponseTracker<Stored>,
    selector: SpreadSelector,
    metadata: Metadata,
    first_offset: LogletOffset,
    records: Arc<[Record]>,
}

impl Appender {
    pub fn new(
        global: Arc<SequencerGlobalState>,
        log_server_manager: RemoteLogServerManager,
        response_tracker: ResponseTracker<Stored>,
        selector: SpreadSelector,
        metadata: Metadata,
        first_offset: LogletOffset,
        records: Arc<[Record]>,
    ) -> Self {
        Self {
            global,
            log_server_manager,
            response_tracker,
            selector,
            metadata,
            first_offset,
            records,
        }
    }

    pub async fn run(mut self) {
        let mut state = AppenderState::Wave {
            replicated: NodeSet::empty(),
            gray_list: NodeSet::empty(),
        };

        loop {
            state = match state {
                AppenderState::Wave {
                    replicated,
                    gray_list,
                } => self.wave(replicated, gray_list).await,
                AppenderState::Done => break,
            }
        }
    }

    async fn wave(&mut self, replicated: NodeSet, mut gray_list: NodeSet) -> AppenderState {
        let spread = match self.selector.select(
            &mut rand::thread_rng(),
            &self.metadata.nodes_config_ref(),
            &gray_list,
        ) {
            Ok(spread) => spread,
            Err(_) => {
                //todo(azmy): retry without a gray-list !
                return AppenderState::Wave {
                    replicated,
                    gray_list: NodeSet::empty(),
                };
            }
        };

        let mut gray = false;
        let mut servers = Vec::with_capacity(spread.len());
        for id in spread {
            // at this stage, if we fail to get connection to this server it must be
            // a first time use. We can safely assume this has to be gray listed
            let server = match self.log_server_manager.get(id).await {
                Ok(server) => server,
                Err(err) => {
                    tracing::error!("Failed to connect to {}: {}", id, err);
                    gray = true;
                    gray_list.insert(id);
                    continue;
                }
            };

            servers.push(server);
        }

        if gray {
            // so not all nodes in the new spread can be used
            // todo(azmy): is it possible that we still can get a write quorum on the remaining
            //    set of nodes. The selector probably should provide an interface to validate that!

            // we basically try again with a new set of nodes
            return AppenderState::Wave {
                replicated,
                gray_list,
            };
        }

        return self.send_wave(servers).await;
    }

    async fn send_wave(&mut self, spread: Vec<RemoteLogServer>) -> AppenderState {
        let mut waiters = JoinSet::new();

        for server in spread {
            // it is possible that we have visited this server
            // in a previous wave. So we can short circuit here
            // and just skip
            if server.tail().latest_offset() > self.first_offset {
                continue;
            }

            let sender = Sender {
                global: Arc::clone(&self.global),
                server_manager: self.log_server_manager.clone(),
                server,
                first_offset: self.first_offset,
                records: Arc::clone(&self.records),
                tracker: self.response_tracker.clone(),
            };

            waiters.spawn(sender.send());
        }

        let mut gray_list = NodeSet::empty();
        let mut replicated = NodeSet::empty();
        // todo(azmy): join_next should timeout if nodes are taking too long to respond!
        while let Ok((server, result)) = waiters.join_next().await.expect("task run to completion")
        {
            let status = match result {
                Ok(status) => status,
                Err(_err) => {
                    // todo(azmy): handle errors differently
                    gray_list.insert(*server.node());
                    continue;
                }
            };

            // we had a response from this node and there is still a lot we can do
            match status {
                Status::Ok => {
                    replicated.insert(*server.node());
                }
                Status::Sealed | Status::Sealing => {
                    server.tail().notify_seal();
                }
                _ => {
                    //todo(azmy): handle other status
                    gray_list.insert(*server.node());
                }
            }
        }

        if gray_list.is_empty() {
            AppenderState::Done
        } else {
            AppenderState::Wave {
                replicated,
                gray_list,
            }
        }
    }
}

struct Sender {
    global: Arc<SequencerGlobalState>,
    server_manager: RemoteLogServerManager,
    server: RemoteLogServer,
    first_offset: LogletOffset,
    records: Arc<[Record]>,
    tracker: ResponseTracker<Stored>,
}

impl Sender {
    async fn send(mut self) -> (RemoteLogServer, Result<Status, NetworkError>) {
        let mut global_tail = self.global.committed_tail().to_stream();
        let mut local_tail = self.server.tail().to_stream();

        loop {
            let tail = tokio::select! {
                Some(tail) = global_tail.next() => {
                    tail
                },
                Some(tail) = local_tail.next() => {
                    tail
                }
            };

            match tail {
                TailState::Sealed(_) => {
                    //either local or global tail is commited,
                    return (self.server, Ok(Status::Sealed));
                }
                TailState::Open(offset) => {
                    if offset > self.first_offset {
                        // somehow the global (of local) offset have moved
                        // behind our first offset!
                        // for now we assume we don't need to make the
                        // write!
                        return (self.server, Ok(Status::Ok));
                    } else if offset == self.first_offset {
                        break;
                    }
                }
            }
        }

        let incoming = match self.try_send().await {
            Ok(incoming) => incoming,
            Err(err) => {
                return (self.server, Err(err));
            }
        };

        // quick actions
        match incoming.status {
            Status::Ok => {
                self.server.tail().notify_offset_update(incoming.local_tail);
            }
            _ => {}
        }

        (self.server, Ok(incoming.status))
    }

    async fn try_send(&mut self) -> Result<Incoming<Stored>, NetworkError> {
        // if we are here so either we at global committed tail or node tail
        // is at first_offset. In either cases, we can try to send our Store message!
        let store = Store {
            first_offset: self.first_offset,
            flags: StoreFlags::empty(),
            known_archived: LogletOffset::INVALID,
            known_global_tail: self.global.committed_tail.latest_offset(),
            loglet_id: self.server.loglet_id(),
            payloads: Vec::from_iter(self.records.iter().cloned()),
            sequencer: self.global.node_id,
            timeout_at: None,
        };

        let mut msg = Outgoing::new(*self.server.node(), store);
        let token = self
            .tracker
            .new_token(msg.msg_id())
            .expect("unique message id");

        loop {
            match self.server.sender().send(msg).await {
                Ok(_) => break,
                Err(send) => {
                    msg = send.message;

                    match send.source {
                        NetworkError::ConnectionClosed
                        | NetworkError::ConnectError(_)
                        | NetworkError::Timeout(_) => {
                            self.server_manager.renew(&mut self.server).await?
                        }
                        _ => return Err(send.source.into()),
                    }
                }
            }
        }

        // message has been sent! there is noway
        // we are sure it has been received by the other peer
        // so we wait for response. indefinitely
        // it's up to the appender time outs to try again
        token.recv().await.map_err(NetworkError::from)
    }
}
