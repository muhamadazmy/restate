// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License,

use clap::Parser;
use cling::{Collect, Run};

use crate::commands::metadata::MetadataCommonOpts;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[clap()]
#[cling(run = "generate_logs")]
pub struct GenerateLogsOpts {
    #[clap(flatten)]
    metadata: MetadataCommonOpts,
}

async fn generate_logs(_opts: &GenerateLogsOpts) -> anyhow::Result<()> {
    //todo: generate and store logs metadata
    Ok(())
}
