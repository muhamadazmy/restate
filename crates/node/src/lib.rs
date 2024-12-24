// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod cluster_marker;
mod init;
mod network_server;
mod roles;

use anyhow::Context;
use tonic::{Code, IntoRequest};
use tracing::{debug, error, info, trace, warn};

use crate::cluster_marker::ClusterValidationError;
use crate::init::NodeInit;
use crate::network_server::NetworkServer;
use crate::roles::{AdminRole, BaseRole, IngressRole, WorkerRole};
use codederror::CodedError;
use restate_bifrost::BifrostService;
use restate_core::metadata_store::ReadWriteError;
use restate_core::network::net_util::create_tonic_channel_from_advertised_address;
use restate_core::network::{
    GrpcConnector, MessageRouterBuilder, NetworkServerBuilder, Networking,
};
use restate_core::partitions::{spawn_partition_routing_refresher, PartitionRoutingRefresher};
use restate_core::protobuf::node_ctl_svc::node_ctl_svc_client::NodeCtlSvcClient;
use restate_core::protobuf::node_ctl_svc::ProvisionClusterRequest;
use restate_core::{cancellation_watcher, Metadata, TaskKind};
use restate_core::{spawn_metadata_manager, MetadataBuilder, MetadataManager, TaskCenter};
#[cfg(feature = "replicated-loglet")]
use restate_log_server::LogServerService;
use restate_metadata_store::local::LocalMetadataStoreService;
use restate_metadata_store::MetadataStoreClient;
use restate_types::config::Configuration;
use restate_types::errors::GenericError;
use restate_types::health::Health;
use restate_types::live::Live;
#[cfg(feature = "replicated-loglet")]
use restate_types::logs::RecordCache;
use restate_types::nodes_config::Role;
use restate_types::protobuf::common::{
    AdminStatus, IngressStatus, LogServerStatus, MetadataServerStatus, NodeRpcStatus, NodeStatus,
    WorkerStatus,
};

#[derive(Debug, thiserror::Error, CodedError)]
pub enum Error {
    #[error("node failed to start due to failed safety check: {0}")]
    #[code(unknown)]
    SafetyCheck(String),
    #[error(
        "missing nodes configuration; can only create it if '--allow-bootstrap true' is specified"
    )]
    #[code(unknown)]
    MissingNodesConfiguration,
    #[error("detected concurrent node registration for node '{0}'; stepping down")]
    #[code(unknown)]
    ConcurrentNodeRegistration(String),
    #[error("could not read/write from/to metadata store: {0}")]
    #[code(unknown)]
    MetadataStore(#[from] ReadWriteError),
}

#[derive(Debug, thiserror::Error, CodedError)]
pub enum BuildError {
    #[error("building worker failed: {0}")]
    Worker(
        #[from]
        #[code]
        roles::WorkerRoleBuildError,
    ),
    #[error("building cluster controller failed: {0}")]
    ClusterController(
        #[from]
        #[code]
        roles::AdminRoleBuildError,
    ),
    #[error("building log-server failed: {0}")]
    LogServer(
        #[from]
        #[code]
        restate_log_server::LogServerBuildError,
    ),
    #[error("node neither runs cluster controller nor its address has been configured")]
    #[code(unknown)]
    UnknownClusterController,
    #[error("cluster bootstrap failed: {0}")]
    #[code(unknown)]
    Bootstrap(String),
    #[error("failed validating and updating cluster marker: {0}")]
    #[code(unknown)]
    ClusterValidation(#[from] ClusterValidationError),

    #[error("failed to initialize metadata store client: {0}")]
    #[code(unknown)]
    MetadataStoreClient(GenericError),

    #[error("building metadata store failed: {0}")]
    #[code(unknown)]
    MetadataStore(#[from] restate_metadata_store::local::BuildError),
}

pub struct Node {
    health: Health,
    server_builder: NetworkServerBuilder,
    updateable_config: Live<Configuration>,
    metadata_manager: MetadataManager,
    partition_routing_refresher: PartitionRoutingRefresher,
    metadata_store_client: MetadataStoreClient,
    bifrost: BifrostService,
    metadata_store_role: Option<LocalMetadataStoreService>,
    base_role: BaseRole,
    admin_role: Option<AdminRole<GrpcConnector>>,
    worker_role: Option<WorkerRole>,
    ingress_role: Option<IngressRole<GrpcConnector>>,
    #[cfg(feature = "replicated-loglet")]
    log_server: Option<LogServerService>,
    networking: Networking<GrpcConnector>,
}

impl Node {
    pub async fn create(updateable_config: Live<Configuration>) -> Result<Self, BuildError> {
        let health = Health::default();
        health.node_status().update(NodeStatus::StartingUp);
        let mut server_builder = NetworkServerBuilder::default();
        let config = updateable_config.pinned();

        cluster_marker::validate_and_update_cluster_marker(config.common.cluster_name())?;

        let metadata_store_role = if config.has_role(Role::MetadataStore) {
            Some(
                LocalMetadataStoreService::create(
                    health.metadata_server_status(),
                    &config.metadata_store,
                    updateable_config
                        .clone()
                        .map(|config| &config.metadata_store.rocksdb)
                        .boxed(),
                    &mut server_builder,
                )
                .await?,
            )
        } else {
            None
        };

        let metadata_store_client = restate_metadata_store::local::create_client(
            config.common.metadata_store_client.clone(),
        )
        .await
        .map_err(BuildError::MetadataStoreClient)?;

        let mut router_builder = MessageRouterBuilder::default();
        let metadata_builder = MetadataBuilder::default();
        let metadata = metadata_builder.to_metadata();
        let networking = Networking::new(metadata_builder.to_metadata(), config.networking.clone());
        let metadata_manager =
            MetadataManager::new(metadata_builder, metadata_store_client.clone());
        metadata_manager.register_in_message_router(&mut router_builder);
        let partition_routing_refresher =
            PartitionRoutingRefresher::new(metadata_store_client.clone());

        #[cfg(feature = "replicated-loglet")]
        let record_cache = RecordCache::new(
            Configuration::pinned()
                .bifrost
                .record_cache_memory_size
                .as_usize(),
        );

        // Setup bifrost
        // replicated-loglet
        #[cfg(feature = "replicated-loglet")]
        let replicated_loglet_factory = restate_bifrost::providers::replicated_loglet::Factory::new(
            metadata_store_client.clone(),
            networking.clone(),
            record_cache.clone(),
            &mut router_builder,
        );
        let bifrost_svc =
            BifrostService::new(metadata_manager.writer()).enable_local_loglet(&updateable_config);

        #[cfg(feature = "replicated-loglet")]
        let bifrost_svc = bifrost_svc.with_factory(replicated_loglet_factory);

        #[cfg(feature = "memory-loglet")]
        let bifrost_svc = bifrost_svc.enable_in_memory_loglet();

        #[cfg(not(feature = "replicated-loglet"))]
        warn_if_log_store_left_artifacts(&config);

        let bifrost = bifrost_svc.handle();

        #[cfg(feature = "replicated-loglet")]
        let log_server = if config.has_role(Role::LogServer) {
            Some(
                LogServerService::create(
                    health.log_server_status(),
                    updateable_config.clone(),
                    metadata.clone(),
                    metadata_store_client.clone(),
                    record_cache,
                    &mut router_builder,
                    &mut server_builder,
                )
                .await?,
            )
        } else {
            None
        };

        let worker_role = if config.has_role(Role::Worker) {
            Some(
                WorkerRole::create(
                    health.worker_status(),
                    metadata.clone(),
                    partition_routing_refresher.partition_routing(),
                    updateable_config.clone(),
                    &mut router_builder,
                    networking.clone(),
                    bifrost_svc.handle(),
                    metadata_store_client.clone(),
                )
                .await?,
            )
        } else {
            None
        };

        let ingress_role = if config
            .ingress
            .experimental_feature_enable_separate_ingress_role
            && config.has_role(Role::HttpIngress)
            // todo remove once the safe fallback version supports the HttpIngress role
            || !config
                .ingress
                .experimental_feature_enable_separate_ingress_role
                && config.has_role(Role::Worker)
        {
            Some(IngressRole::create(
                updateable_config
                    .clone()
                    .map(|config| &config.ingress)
                    .boxed(),
                health.ingress_status(),
                networking.clone(),
                metadata.updateable_schema(),
                metadata.updateable_partition_table(),
                partition_routing_refresher.partition_routing(),
                &mut router_builder,
            ))
        } else {
            None
        };

        let admin_role = if config.has_role(Role::Admin) {
            Some(
                AdminRole::create(
                    health.admin_status(),
                    bifrost.clone(),
                    updateable_config.clone(),
                    partition_routing_refresher.partition_routing(),
                    networking.clone(),
                    metadata,
                    metadata_manager.writer(),
                    &mut server_builder,
                    &mut router_builder,
                    worker_role
                        .as_ref()
                        .map(|worker_role| worker_role.storage_query_context().clone()),
                )
                .await?,
            )
        } else {
            None
        };

        let base_role = BaseRole::create(
            &mut router_builder,
            worker_role
                .as_ref()
                .map(|role| role.partition_processor_manager_handle()),
        );

        // Ensures that message router is updated after all services have registered themselves in
        // the builder.
        let message_router = router_builder.build();
        networking
            .connection_manager()
            .set_message_router(message_router);

        Ok(Node {
            health,
            updateable_config,
            metadata_manager,
            partition_routing_refresher,
            bifrost: bifrost_svc,
            metadata_store_role,
            metadata_store_client,
            base_role,
            admin_role,
            ingress_role,
            worker_role,
            #[cfg(feature = "replicated-loglet")]
            log_server,
            server_builder,
            networking,
        })
    }

    pub async fn start(self) -> Result<(), anyhow::Error> {
        let config = self.updateable_config.pinned();

        let metadata_writer = self.metadata_manager.writer();
        let metadata = self.metadata_manager.metadata().clone();
        let is_set = TaskCenter::try_set_global_metadata(metadata.clone());
        debug_assert!(is_set, "Global metadata was already set");

        // Start metadata manager
        spawn_metadata_manager(self.metadata_manager)?;

        // spawn the node rpc server first to enable connecting to the metadata store
        TaskCenter::spawn(TaskKind::RpcServer, "node-rpc-server", {
            let health = self.health.clone();
            let common_options = config.common.clone();
            let connection_manager = self.networking.connection_manager().clone();
            let metadata_store_client = self.metadata_store_client.clone();
            async move {
                NetworkServer::run(
                    health,
                    connection_manager,
                    self.server_builder,
                    common_options,
                    metadata_store_client,
                )
                .await?;
                Ok(())
            }
        })?;

        // wait until the node rpc server is up and running before continuing
        self.health
            .node_rpc_status()
            .wait_for_value(NodeRpcStatus::Ready)
            .await;

        if let Some(metadata_store) = self.metadata_store_role {
            TaskCenter::spawn(TaskKind::MetadataStore, "metadata-store", async move {
                metadata_store.run().await?;
                Ok(())
            })?;
        }

        if config.common.allow_bootstrap {
            TaskCenter::spawn(TaskKind::SystemBoot, "auto-provision-cluster", {
                let channel = create_tonic_channel_from_advertised_address(
                    config.common.advertised_address.clone(),
                    &config.networking,
                );
                let client = NodeCtlSvcClient::new(channel);
                let retry_policy = config.common.network_error_retry_policy.clone();
                async move {
                    let response = retry_policy
                        .retry_if(
                            || {
                                let mut client = client.clone();
                                async move {
                                    client
                                        .provision_cluster(
                                            ProvisionClusterRequest {
                                                dry_run: false,
                                                ..Default::default()
                                            }
                                            .into_request(),
                                        )
                                        .await
                                }
                            },
                            |status| status.code() == Code::Unavailable,
                        )
                        .await;

                    match response {
                        Ok(response) => {
                            let response = response.into_inner();
                            debug_assert!(!response.dry_run, "Provision w/o dry run");
                        }
                        Err(err) => {
                            if err.code() == Code::AlreadyExists {
                                debug!("The cluster is already provisioned.")
                            } else {
                                warn!("Failed to auto provision the cluster. In order to continue you have to provision the cluster manually: {err}");
                            }
                        }
                    }

                    Ok(())
                }
            })?;
        }

        let initialization_timeout = config.common.initialization_timeout.into();

        tokio::time::timeout(
            initialization_timeout,
            NodeInit::new(
                &self.metadata_store_client,
                &metadata_writer,
            )
            .init(),
        )
        .await
            .context("Giving up trying to initialize the node. Make sure that it can reach the metadata store and don't forget to provision the cluster on a fresh start.")?
            .context("Failed initializing the node.")?;

        let nodes_config = Metadata::with_current(|m| m.nodes_config_ref());
        let my_node_id = Metadata::with_current(|m| m.my_node_id());
        let my_node_config = nodes_config
            .find_node_by_id(my_node_id)
            .expect("should be present");

        // Start partition routing information refresher
        spawn_partition_routing_refresher(self.partition_routing_refresher)?;

        // todo this is a temporary solution to announce the updated NodesConfiguration to the
        //  configured admin nodes. It should be removed once we have a gossip-based node status
        //  protocol. Notifying the admin nodes is done on a best effort basis in case one admin
        //  node is currently not available. This is ok, because the admin nodes will periodically
        //  sync the NodesConfiguration from the metadata store themselves.
        for admin_node in nodes_config.get_admin_nodes() {
            // we don't have to announce us to ourselves
            if admin_node.current_generation != my_node_id {
                let admin_node_id = admin_node.current_generation;
                let networking = self.networking.clone();

                TaskCenter::spawn_unmanaged(
                    TaskKind::Disposable,
                    "announce-node-at-admin-node",
                    async move {
                        if let Err(err) = networking.node_connection(admin_node_id).await {
                            info!("Failed connecting to admin node '{admin_node_id}' and announcing myself. This can indicate network problems: {err}");
                        }
                    },
                )?;
            }
        }

        // Ensures bifrost has initial metadata synced up before starting the worker.
        // Need to run start in new tc scope to have access to metadata()
        self.bifrost.start().await?;

        #[cfg(feature = "replicated-loglet")]
        if let Some(log_server) = self.log_server {
            log_server.start(metadata_writer).await?;
        }

        if let Some(admin_role) = self.admin_role {
            TaskCenter::spawn(TaskKind::SystemBoot, "admin-init", admin_role.start())?;
        }

        if let Some(worker_role) = self.worker_role {
            TaskCenter::spawn(TaskKind::SystemBoot, "worker-init", worker_role.start())?;
        }

        if let Some(ingress_role) = self.ingress_role {
            TaskCenter::spawn_child(TaskKind::Ingress, "ingress-http", ingress_role.run())?;
        }

        self.base_role.start()?;

        let node_status = self.health.node_status();
        node_status.update(NodeStatus::Alive);

        let my_roles = my_node_config.roles;
        // Report that the node is running when all roles are ready
        let _ = TaskCenter::spawn(TaskKind::Disposable, "status-report", async move {
            self.health
                .node_rpc_status()
                .wait_for_value(NodeRpcStatus::Ready)
                .await;
            trace!("Node-to-node networking is ready");
            for role in my_roles {
                match role {
                    Role::Worker => {
                        self.health
                            .worker_status()
                            .wait_for_value(WorkerStatus::Ready)
                            .await;
                        trace!("Worker role is reporting ready");
                    }
                    Role::Admin => {
                        self.health
                            .admin_status()
                            .wait_for_value(AdminStatus::Ready)
                            .await;
                        trace!("Worker role is reporting ready");
                    }
                    Role::MetadataStore => {
                        self.health
                            .metadata_server_status()
                            .wait_for_value(MetadataServerStatus::Ready)
                            .await;
                        trace!("Metadata role is reporting ready");
                    }

                    Role::LogServer => {
                        self.health
                            .log_server_status()
                            .wait_for_value(LogServerStatus::Ready)
                            .await;
                        trace!("Log-server is reporting ready");
                    }
                    Role::HttpIngress => {
                        self.health
                            .ingress_status()
                            .wait_for_value(IngressStatus::Ready)
                            .await;
                        trace!("Ingress is reporting ready");
                    }
                }
            }
            info!("Restate server is ready");
            Ok(())
        });

        let _ = TaskCenter::spawn_child(TaskKind::Background, "node-status", async move {
            cancellation_watcher().await;
            node_status.update(NodeStatus::ShuttingDown);
            Ok(())
        });

        Ok(())
    }

    pub fn bifrost(&self) -> restate_bifrost::Bifrost {
        self.bifrost.handle()
    }

    pub fn metadata_store_client(&self) -> MetadataStoreClient {
        self.metadata_store_client.clone()
    }

    pub fn metadata_writer(&self) -> restate_core::MetadataWriter {
        self.metadata_manager.writer()
    }
}

#[cfg(not(feature = "replicated-loglet"))]
fn warn_if_log_store_left_artifacts(config: &Configuration) {
    if config.log_server.data_dir().exists() {
        tracing::warn!("Log server data directory '{}' exists, \
            but log-server is not implemented in this version of restate-server. \
            This may indicate that the log-server role was previously enabled and the data directory was not cleaned up. If this was created by v1.1.1 of restate-server, please remove this directory to avoid potential future conflicts.", config.log_server.data_dir().display());
    }
}
