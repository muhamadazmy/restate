// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, hash_map};
use std::sync::Arc;

use async_trait::async_trait;
use tracing::debug;
use tokio::sync::Mutex;

use restate_types::config::LocalLogletOptions;
use restate_types::live::BoxLiveLoad;
use restate_types::logs::metadata::{
    Chain, LogletParams, ProviderConfiguration, ProviderKind, SegmentIndex,
};
use restate_types::logs::{LogId, LogletId};

use super::log_store::RocksDbLogStore;
use super::log_store_writer::RocksDbLogWriterHandle;
use super::{LocalLoglet, metric_definitions};
use crate::Error;
use crate::loglet::{Loglet, LogletProvider, LogletProviderFactory, OperationError};

pub struct Factory {
    options: BoxLiveLoad<LocalLogletOptions>,
}

impl Factory {
    pub fn new(options: BoxLiveLoad<LocalLogletOptions>) -> Self {
        Self { options }
    }
}

#[async_trait]
impl LogletProviderFactory for Factory {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Local
    }

    async fn create(self: Box<Self>) -> Result<Arc<dyn LogletProvider>, OperationError> {
        metric_definitions::describe_metrics();
        let Factory { options } = *self;
        
        debug!("Started a bifrost local loglet provider");
        Ok(Arc::new(LocalLogletProvider {
            options,
            log_store: Mutex::new(None),
            active_loglets: Default::default(),
        }))
    }
}

#[derive(Clone)]
struct RocksDbLogStoreBundle {
    log_store: RocksDbLogStore,
    log_writer: RocksDbLogWriterHandle,
}

pub(crate) struct LocalLogletProvider {
    options: BoxLiveLoad<LocalLogletOptions>,
    log_store: Mutex<Option<RocksDbLogStoreBundle>>,
    active_loglets: Mutex<HashMap<(LogId, SegmentIndex), Arc<LocalLoglet>>>,
}

#[async_trait]
impl LogletProvider for LocalLogletProvider {
    async fn get_loglet(
        &self,
        log_id: LogId,
        segment_index: SegmentIndex,
        params: &LogletParams,
    ) -> Result<Arc<dyn Loglet>, Error> {
        let mut guard = self.active_loglets.lock().await;
        let loglet = match guard.entry((log_id, segment_index)) {
            hash_map::Entry::Vacant(entry) => {
                let mut log_store_guard = self.log_store.lock().await;
                let log_store = match &*log_store_guard {
                    Some(log_store) => {
                        log_store.clone()
                    }
                    None => {
                        debug!("Creating local loglet log store");
                        let log_store = RocksDbLogStore::create(self.options.clone())
                            .await
                            .map_err(OperationError::other)?;

                        let log_writer = log_store.create_writer().start(self.options.clone())?;

                        let log_store = RocksDbLogStoreBundle {
                            log_store,
                            log_writer,
                        };

                        *log_store_guard = Some(log_store.clone());
                        log_store
                    }
                };
                
                // Create loglet
                // NOTE: local-loglet expects params to be a `u64` string-encoded unique identifier under the hood.
                let loglet = LocalLoglet::create(
                    params
                        .parse()
                        .expect("loglet params can be converted into u64"),
                    log_store.log_store,
                    log_store.log_writer,
                )?;
                let loglet = entry.insert(Arc::new(loglet));
                Arc::clone(loglet)
            }
            hash_map::Entry::Occupied(entry) => entry.get().clone(),
        };

        Ok(loglet as Arc<dyn Loglet>)
    }

    fn propose_new_loglet_params(
        &self,
        log_id: LogId,
        chain: Option<&Chain>,
        _defaults: &ProviderConfiguration,
    ) -> Result<LogletParams, OperationError> {
        let new_segment_index = chain
            .map(|c| c.tail_index().next())
            .unwrap_or(SegmentIndex::OLDEST);
        Ok(LogletParams::from(
            u64::from(LogletId::new(log_id, new_segment_index)).to_string(),
        ))
    }

    async fn shutdown(&self) -> Result<(), OperationError> {
        Ok(())
    }
}
