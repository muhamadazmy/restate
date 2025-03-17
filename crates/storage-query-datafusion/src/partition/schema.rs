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
    /// Partition placements and their target running mode
    partition(
    /// Partition ID
    id: DataType::UInt32,

    /// Node ID where the partition is running
    node_id: DataType::UInt32,

    /// Target run mode of partition (LEADER, FOLLOWER)
    target_mode: DataType::Utf8,

    /// Partition start key
    start_key: DataType::UInt64,

    /// Partition end key
    end_key: DataType::UInt64,
));
