// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod client;
mod interface;

pub use self::client::CloudClient;
pub use self::interface::CloudClientInterface;

pub mod generated {
    #![allow(clippy::to_string_trait_impl)]

    use serde::{Deserialize, Serialize};

    typify::import_types!(schema = "src/clients/cloud/schema.json");
}
