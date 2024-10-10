// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytestring::ByteString;
use clap::Parser;
use cling::{Collect, Run};
use restate_types::storage::StorageDecode;
use restate_types::Versioned;
use tracing::debug;

use restate_rocksdb::RocksDbManager;
use restate_types::config::Configuration;
use restate_types::live::Live;

use crate::commands::metadata::{
    create_metadata_store_client, GenericMetadataValue, MetadataAccessMode, MetadataCommonOpts,
};
use crate::environment::metadata_store;
use crate::environment::task_center::run_in_task_center;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[clap()]
#[cling(run = "show_value")]
pub struct GetValueOpts {
    #[clap(flatten)]
    pub(crate) metadata: MetadataCommonOpts,

    /// The key to get
    #[arg(short, long)]
    pub(crate) key: String,
}

pub(crate) async fn get_value<K, T>(opts: &MetadataCommonOpts, key: K) -> anyhow::Result<Option<T>>
where
    K: AsRef<str>,
    T: Versioned + StorageDecode,
{
    let value = match opts.access_mode {
        MetadataAccessMode::Remote => get_value_remote(opts, key).await?,
        MetadataAccessMode::Direct => get_value_direct(opts, key).await?,
    };

    Ok(value)
}

pub(crate) async fn show_value(opts: &GetValueOpts) -> anyhow::Result<()> {
    let value: Option<GenericMetadataValue> = get_value(&opts.metadata, &opts.key).await?;

    serde_json::to_writer_pretty(std::io::stdout(), &value)?;
    Ok(())
}

async fn get_value_remote<K, T>(opts: &MetadataCommonOpts, key: K) -> anyhow::Result<Option<T>>
where
    K: AsRef<str>,
    T: Versioned + StorageDecode,
{
    let metadata_store_client = create_metadata_store_client(opts).await?;

    metadata_store_client
        .get(ByteString::from(key.as_ref()))
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get value: {}", e))
}

async fn get_value_direct<K, T>(opts: &MetadataCommonOpts, key: K) -> anyhow::Result<Option<T>>
where
    K: AsRef<str>,
    T: Versioned + StorageDecode,
{
    run_in_task_center(
        opts.config_file.as_ref(),
        |config, task_center| async move {
            let rocksdb_manager =
                RocksDbManager::init(Configuration::mapped_updateable(|c| &c.common));
            debug!("RocksDB Initialized");

            let metadata_store_client = metadata_store::start_metadata_store(
                config.common.metadata_store_client.clone(),
                Live::from_value(config.metadata_store.clone()).boxed(),
                Live::from_value(config.metadata_store.clone())
                    .map(|c| &c.rocksdb)
                    .boxed(),
                &task_center,
            )
            .await?;
            debug!("Metadata store client created");

            let value = metadata_store_client
                .get(ByteString::from(key.as_ref()))
                .await
                .map_err(|e| anyhow::anyhow!("Failed to get value: {}", e))?;

            rocksdb_manager.shutdown().await;
            anyhow::Ok(value)
        },
    )
    .await
}
