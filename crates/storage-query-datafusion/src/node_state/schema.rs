// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(dead_code)]

use datafusion::arrow::datatypes::DataType;

use crate::table_macro::*;

define_table!(
    /// Observed node status
    node_state(
        /// Node ID
        id: DataType::UInt32,

        /// Current observed generation ID
        generation: DataType::UInt32,

        /// Node observed status
        status: DataType::Utf8,

        /// Node uptime (if ALIVE)
        uptime: DataType::UInt64,

        /// Node last seen timestamp
        last_seen_ts: DataType::UInt64,

        /// Last attempt to reach the node timestamp (if SUSPECT)
        last_attempt_ts: DataType::UInt64,
    )
);
