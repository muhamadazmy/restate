// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod describe_log;
mod dump_log;
mod find_tail;
mod gen_metadata;
mod list_logs;
mod reconfigure;
mod trim_log;

use std::{ops::RangeInclusive, str::FromStr};

use cling::prelude::*;

#[derive(Run, Subcommand, Clone)]
pub enum Log {
    /// List the logs by partition
    List(list_logs::ListLogsOpts),
    /// Prints a generated log-metadata in JSON format
    GenerateMetadata(gen_metadata::GenerateLogMetadataOpts),
    /// Get the details of a specific log
    Describe(describe_log::DescribeLogIdOpts),
    /// Dump the contents of a bifrost log
    Dump(dump_log::DumpLogOpts),
    /// Trim a log to a particular Log Sequence Number (LSN)
    Trim(trim_log::TrimLogOpts),
    /// Reconfigure a log by sealing the tail segment
    /// and extending the chain with a new one
    Reconfigure(reconfigure::ReconfigureOpts),
    /// Find and show tail state of a log
    FindTail(find_tail::FindTailOpts),
}

#[derive(Parser, Collect, Clone, Debug)]
struct LogIdRange {
    from: u32,
    to: u32,
}

impl LogIdRange {
    fn new(from: u32, to: u32) -> anyhow::Result<Self> {
        if from > to {
            anyhow::bail!(
                "Invalid log id range: {}..{}, start must be <= end range",
                from,
                to
            )
        } else {
            Ok(LogIdRange { from, to })
        }
    }

    fn iter(&self) -> impl Iterator<Item = u32> {
        self.from..=self.to
    }
}

impl IntoIterator for LogIdRange {
    type IntoIter = RangeInclusive<u32>;
    type Item = u32;
    fn into_iter(self) -> Self::IntoIter {
        self.from..=self.to
    }
}

impl FromStr for LogIdRange {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split("-").collect();
        match parts.len() {
            1 => {
                let n = parts[0].parse()?;
                Ok(LogIdRange::new(n, n)?)
            }
            2 => {
                let from = parts[0].parse()?;
                let to = parts[1].parse()?;
                Ok(LogIdRange::new(from, to)?)
            }
            _ => anyhow::bail!("Invalid log id or log range: {}", s),
        }
    }
}
