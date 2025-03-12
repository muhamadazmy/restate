// Copyright (c) 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use futures::future::BoxFuture;
use tonic::{body::BoxBody, server::NamedService};

/// Injects a version header to any grpc service
pub struct ApiVersionLayer {
    min_version: Option<&'static str>,
    version: &'static str,
}

impl ApiVersionLayer {
    pub fn new(version: &'static str) -> Self {
        Self {
            min_version: None,
            version,
        }
    }

    pub fn with_min_version(version: &'static str, min_version: &'static str) -> Self {
        Self {
            min_version: Some(min_version),
            version,
        }
    }
}

impl Default for ApiVersionLayer {
    fn default() -> Self {
        Self::new(env!("CARGO_PKG_VERSION"))
    }
}

impl<S> tower::Layer<S> for ApiVersionLayer {
    type Service = WithVersionService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        WithVersionService {
            min_version: self.min_version,
            version: self.version,
            inner,
        }
    }
}

#[derive(Clone)]
pub struct WithVersionService<S> {
    min_version: Option<&'static str>,
    version: &'static str,
    inner: S,
}

impl<S> WithVersionService<S> {
    const VERSION_HEADER: &'static str = "X-Api-Version";
    const MIN_VERSION_HEADER: &'static str = "X-Api-Min-Version";
}

impl<S: NamedService> NamedService for WithVersionService<S> {
    const NAME: &'static str = S::NAME;
}

impl<S> tower::Service<http::Request<BoxBody>> for WithVersionService<S>
where
    S: tower::Service<http::Request<BoxBody>, Response = http::Response<BoxBody>> + Send + 'static,
    S::Future: Send + 'static,
{
    type Error = S::Error;
    type Response = S::Response;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<BoxBody>) -> Self::Future {
        let fut = self.inner.call(req);

        let ver = self.version;
        let min_ver = self.min_version;
        Box::pin(async move {
            let mut result = fut.await;
            if let Ok(response) = &mut result {
                response
                    .headers_mut()
                    .insert(Self::VERSION_HEADER, ver.parse().expect("valid string"));

                if let Some(min_ver) = min_ver {
                    response.headers_mut().insert(
                        Self::MIN_VERSION_HEADER,
                        min_ver.parse().expect("valid string"),
                    );
                }
            }
            result
        })
    }
}
