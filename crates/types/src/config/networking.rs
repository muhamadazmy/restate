// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use crate::retries::RetryPolicy;

/// # Networking options
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(rename = "NetworkingOptions", default))]
#[serde(rename_all = "kebab-case")]
#[builder(default)]
pub struct NetworkingOptions {
    /// # Retry policy
    ///
    /// Retry policy to use for internal node-to-node networking.
    pub connect_retry_policy: RetryPolicy,

    /// # Connection Send Buffer
    ///
    /// The number of messages that can be queued on the outbound stream of a single
    /// connection
    pub outbound_queue_length: NonZeroUsize,

    /// # Handshake timeout
    ///
    /// Timeout for handshake message for internal node-to-node networking.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub handshake_timeout: humantime::Duration,
}

impl Default for NetworkingOptions {
    fn default() -> Self {
        Self {
            connect_retry_policy: RetryPolicy::exponential(
                Duration::from_millis(250),
                2.0,
                Some(10),
                Some(Duration::from_millis(3000)),
            ),

            outbound_queue_length: NonZeroUsize::new(1000).expect("Non zero number"),
            handshake_timeout: Duration::from_secs(3).into(),
        }
    }
}
