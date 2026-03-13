// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::{NonZeroU32, NonZeroUsize};

/// Configuration for an [`AuthorityPool`].
#[derive(Debug, Clone, derive_builder::Builder)]
#[builder(
    pattern = "owned",
    build_fn(name = "build_inner", private),
    name = "PoolBuilder"
)]
pub struct PoolConfig {
    /// Maximum number of connections to open to a single authority.
    #[builder(default = NonZeroUsize::new(1).unwrap())]
    pub(crate) max_connections: NonZeroUsize,
    /// Initial max H2 send streams per connection (passed to [`Connection::new`]).
    #[builder(default = NonZeroU32::new(100).unwrap())]
    pub(crate) init_max_streams: NonZeroU32,
}

impl PoolBuilder {
    pub fn build<C>(self, connector: C) -> super::Pool<C> {
        let config = self.build_inner().unwrap();
        super::Pool::new(connector, config)
    }
}
