// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::future::Future;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use http::{Request, StatusCode, Uri};
use http_body_util::BodyExt;
use hyper::Response;
use hyper::body::Incoming;
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::connect::HttpConnector;
use reqwest::Body;
use restate_cli_util::CliContext;
use restate_types::retries::RetryPolicy;
use tokio::sync::{OwnedRwLockWriteGuard, RwLock};
use tokio_util::sync::CancellationToken;
use tower::Service;
use tracing::{error, info};
use url::Url;

use crate::cli_env::CliEnv;
use crate::clients::cloud::generated::DescribeEnvironmentResponse;

use super::renderer::{LocalRenderer, TunnelRenderer};

pub(crate) async fn run_local(
    env: &CliEnv,
    client: reqwest::Client,
    bearer_token: &str,
    environment_info: DescribeEnvironmentResponse,
    opts: &super::Tunnel,
    tunnel_renderer: Arc<TunnelRenderer>,
) -> Result<(), ServeError> {
    let port = if let Some(port) = opts.local_port {
        port
    } else {
        return Ok(());
    };

    let mut http_connector = HttpConnector::new();
    http_connector.set_nodelay(true);
    http_connector.set_connect_timeout(Some(CliContext::get().connect_timeout()));
    http_connector.enforce_http(false);
    // default interval on linux is 75 secs, also use this as the start-after
    http_connector.set_keepalive(Some(Duration::from_secs(75)));

    let https_connector = hyper_rustls::HttpsConnectorBuilder::new()
        .with_native_roots()
        .map_err(ServeError::NativeRoots)?
        .https_or_http()
        .enable_http2()
        .wrap_connector(http_connector);

    let request_identity_key =
        super::request_identity::parse_public_key(&environment_info.signing_public_key)
            .expect("Problem validating request identity public key");

    let retry_policy = RetryPolicy::exponential(
        Duration::from_millis(10),
        2.0,
        None,
        Some(Duration::from_secs(12)),
    );

    // use a closure as impl trait associated types are not yet allowed
    let proxy = |inner, request| async move { proxy(inner, request).await };

    let url = Url::parse(&format!("http://localhost:{port}")).unwrap();

    let handler = Handler {
        status: HandlerStatus::AwaitingStart,
        https_connector,
        inner: Arc::new(HandlerInner {
            tunnel_renderer,
            request_identity_key,
            environment_id: environment_info.environment_id,
            environment_name: environment_info.name,
            bearer_token: bearer_token.into(),
            client,
            url,
            opts: opts.clone(),
        }),
        proxy,
    };

    retry_policy
        .retry_if(
            || {
                let tunnel_url = handler
                    .tunnel_uri()
                    .unwrap_or(env.config.cloud.tunnel_base_url.as_str())
                    .parse()
                    .unwrap();

                let handler = handler.clone();

                async move { handler.serve(tunnel_url).await }
            },
            |err: &ServeError| {
                if !err.is_retryable() {
                    return false;
                }

                if handler.inner.tunnel_renderer.local.get().is_none() {
                    // no point retrying if we've never had a success; leave that up to the user
                    return false;
                };

                let err = match err.source() {
                    Some(source) => format!("{err}, retrying\n  Caused by: {source}"),
                    None => format!("{err}, retrying"),
                };
                handler.inner.tunnel_renderer.store_error(err);

                true
            },
        )
        .await
}

pub(crate) struct Handler<Proxy> {
    pub status: HandlerStatus,
    pub https_connector: HttpsConnector<HttpConnector>,
    pub inner: Arc<HandlerInner>,
    pub proxy: Proxy,
}

impl<Proxy> Handler<Proxy> {
    fn tunnel_name(&self) -> Option<String> {
        if let Some(local_renderer) = self.inner.tunnel_renderer.local.get() {
            // already had a tunnel at a particular name, continue to use it
            Some(local_renderer.tunnel_name.clone())
        } else {
            // no tunnel yet; return the user-provided tunnel name if any
            self.inner.opts.tunnel_name.clone()
        }
    }

    pub fn tunnel_uri(&self) -> Option<&str> {
        if let Some(tunnel_renderer) = self.inner.tunnel_renderer.local.get() {
            // already had a tunnel at a particular url, continue to use it
            Some(&tunnel_renderer.tunnel_url)
        } else {
            // no tunnel yet; return the user-provided tunnel url if any
            self.inner.opts.tunnel_url.as_deref()
        }
    }
}

impl<Proxy: Clone> Clone for Handler<Proxy> {
    fn clone(&self) -> Self {
        Self {
            status: HandlerStatus::AwaitingStart,
            https_connector: self.https_connector.clone(),
            inner: self.inner.clone(),
            proxy: self.proxy.clone(),
        }
    }
}

pub(crate) struct HandlerInner {
    pub opts: super::Tunnel,
    pub tunnel_renderer: Arc<TunnelRenderer>,
    pub request_identity_key: jsonwebtoken::DecodingKey,
    pub environment_id: String,
    pub bearer_token: String,
    pub client: reqwest::Client,
    pub environment_name: String,
    pub url: Url,
}

#[derive(Clone, Debug, thiserror::Error)]
pub(crate) enum StartError {
    #[error("Missing trailers")]
    MissingTrailers,
    #[error("Problem reading http2 stream")]
    Read,
    #[error("Bad status: {0}")]
    BadStatus(String),
    #[error("Missing status")]
    MissingStatus,
    #[error("Missing proxy URL")]
    MissingProxyURL,
    #[error("Missing tunnel URL")]
    MissingTunnelURL,
    #[error("Missing tunnel name")]
    MissingTunnelName,
    #[error("Tunnel service provided a different tunnel name to what we requested")]
    TunnelNameMismatch,
    #[error("Timed out while waiting for tunnel handshake")]
    Timeout,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ServeError {
    #[error("Failed to initialise tunnel")]
    StartError(#[from] StartError),
    #[error("Failed to read native SSL roots")]
    NativeRoots(#[source] std::io::Error),
    #[error("Failed to serve over tunnel")]
    Hyper(#[source] hyper::Error),
    #[error("Failed to connect to tunnel server")]
    Connection(#[source] Box<dyn Error + Send + Sync>),
    #[error("Server closed connection while {0}")]
    ServerClosed(String),
}

impl ServeError {
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::StartError(_) => false,
            Self::NativeRoots(_) => false,
            Self::Hyper(_) => false,
            Self::Connection(_) => true,
            Self::ServerClosed(_) => true,
        }
    }
}

impl From<hyper::Error> for ServeError {
    fn from(err: hyper::Error) -> Self {
        if let Some(err) = err
            .source()
            .and_then(|err| err.downcast_ref::<StartError>())
        {
            // this can happen when ProcessingStart future returns an error to poll_ready
            Self::StartError(err.clone())
        } else {
            // generic hyper serving error; not sure how to hit this
            Self::Hyper(err)
        }
    }
}

pub(crate) enum HandlerStatus {
    AwaitingStart,
    Proxying,
    Failed(StartError),
}

impl Display for HandlerStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            HandlerStatus::AwaitingStart => write!(f, "awaiting start"),
            HandlerStatus::Failed(_) => write!(f, "failed"),
            HandlerStatus::Proxying => write!(f, "proxying"),
        }
    }
}

impl<Proxy, ProxyFut> Handler<Proxy>
where
    Proxy: Fn(Arc<HandlerInner>, reqwest::Request) -> ProxyFut + Send + Sync + 'static,
    ProxyFut: Future<Output = Result<Response<Body>, StartError>> + Send + 'static,
{
    pub async fn serve(mut self, tunnel_url: Uri) -> Result<(), ServeError> {
        let io = self
            .https_connector
            .call(tunnel_url)
            .await
            .map_err(ServeError::Connection)?;

        let this = Arc::new(RwLock::new(self));

        let token = CancellationToken::new();
        {
            let server =
                hyper::server::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new())
                    .serve_connection(
                        io,
                        hyper::service::service_fn(|req| {
                            let this = this.clone();
                            let token = token.clone();
                            async move {
                                let guard = this.read().await;
                                match &guard.status {
                                    HandlerStatus::AwaitingStart => {
                                        drop(guard);
                                        let guard = this.write_owned().await;
                                        match &guard.status {
                                            // won the race; process start
                                            HandlerStatus::AwaitingStart => {
                                                Self::process_start(guard, req, token)
                                            }
                                            // lost the race to someone that failed
                                            HandlerStatus::Failed(err) => Err(err.clone()),
                                            // lost the race but they succeeded; treat this as a normal proxy request
                                            HandlerStatus::Proxying => {
                                                let guard = guard.downgrade();
                                                guard.proxy(req).await
                                            }
                                        }
                                    }
                                    HandlerStatus::Proxying => guard.proxy(req).await,
                                    HandlerStatus::Failed(err) => Err(err.clone()),
                                }
                            }
                        }),
                    );

            tokio::select! {
                server_result = server => server_result?,
                _ = token.cancelled() => {},
            }
        }

        let this = this.read().await;

        if let HandlerStatus::Failed(err) = &this.status {
            Err(err.clone().into())
        } else {
            Err(ServeError::ServerClosed(this.status.to_string()))
        }
    }

    fn process_start(
        mut this: OwnedRwLockWriteGuard<Self>,
        req: Request<Incoming>,
        token: CancellationToken,
    ) -> Result<Response<Body>, StartError> {
        let body = req.into_body();

        let resp = Response::builder()
            .header(
                "authorization",
                format!("Bearer {}", this.inner.bearer_token),
            )
            .header("environment-id", &this.inner.environment_id);

        let resp = if let Some(tunnel_name) = this.tunnel_name() {
            resp.header("tunnel-name", tunnel_name)
        } else {
            resp
        };

        // keep holding the lock until this is complete; no other requests should be processed
        tokio::task::spawn(async move {
            match tokio::time::timeout(
                Duration::from_secs(5),
                process_start(this.inner.clone(), body),
            )
            .await
            {
                Ok(Ok(())) => {
                    this.status = HandlerStatus::Proxying;
                }
                Ok(Err(err)) => {
                    this.status = HandlerStatus::Failed(err);
                    token.cancel();
                }
                Err(_timeout) => {
                    this.status = HandlerStatus::Failed(StartError::Timeout);
                    token.cancel();
                }
            }
        });

        Ok(resp.body(Body::default()).unwrap())
    }

    fn proxy(&self, req: Request<Incoming>) -> ProxyFut {
        let url = if let Some(path) = req.uri().path_and_query() {
            self.inner.url.join(path.as_str()).unwrap()
        } else {
            self.inner.url.clone()
        };

        info!("Proxying request to {}", url);

        let (head, body) = req.into_parts();

        let request = self
            .inner
            .client
            .request(head.method, url)
            .body(Body::wrap_stream(body.into_data_stream()))
            .headers(head.headers)
            .build()
            .expect("Failed to build request");

        (self.proxy)(self.inner.clone(), request)
    }
}

async fn process_start(inner: Arc<HandlerInner>, body: Incoming) -> Result<(), StartError> {
    let collected = body.collect().await;
    let trailers = match collected {
        Ok(ref collected) if collected.trailers().is_some() => collected.trailers().unwrap(),
        Ok(_) => {
            return Err(StartError::MissingTrailers);
        }
        Err(_) => {
            return Err(StartError::Read);
        }
    };

    match trailers.get("tunnel-status").and_then(|s| s.to_str().ok()) {
        Some("ok") => {}
        Some(other) => {
            return Err(StartError::BadStatus(other.into()));
        }
        None => {
            return Err(StartError::MissingStatus);
        }
    }

    let proxy_url = match trailers.get("proxy-url").and_then(|s| s.to_str().ok()) {
        Some(url) => url,
        None => {
            return Err(StartError::MissingProxyURL);
        }
    };

    let tunnel_url = match trailers.get("tunnel-url").and_then(|s| s.to_str().ok()) {
        Some(url) => url,
        None => {
            return Err(StartError::MissingTunnelURL);
        }
    };

    let proxy_port = Url::parse(proxy_url)
        .expect("proxy url must be valid")
        .port()
        .expect("proxy url must have a port");

    let tunnel_name = match trailers.get("tunnel-name").and_then(|s| s.to_str().ok()) {
        Some(name) => name,
        None => {
            return Err(StartError::MissingTunnelName);
        }
    };

    if let Some(requested_tunnel_name) = inner.opts.tunnel_name.as_deref() {
        // the user provided a particular tunnel name; check that the server respected this
        if tunnel_name != requested_tunnel_name {
            return Err(StartError::TunnelNameMismatch);
        }
    }

    inner.tunnel_renderer.local.get_or_init(|| {
        LocalRenderer::new(
            proxy_port,
            tunnel_url.into(),
            tunnel_name.into(),
            inner.environment_name.clone(),
            inner.opts.local_port.unwrap(),
        )
    });

    inner.tunnel_renderer.clear_error();

    Ok(())
}

#[derive(Debug, thiserror::Error)]
enum ProxyError {
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
    #[error(
        "Failed to validate request identity.\nCould a different environment be trying to use your tunnel?\n{0}"
    )]
    RequestIdentity(#[from] super::request_identity::Error),
}

pub(crate) async fn proxy(
    inner: Arc<HandlerInner>,
    request: reqwest::Request,
) -> Result<Response<Body>, StartError> {
    if let Err(err) = super::request_identity::validate_request_identity(
        &inner.request_identity_key,
        request.headers(),
        request.url().path(),
    ) {
        error!("Failed to validate request identity: {}", err);

        inner.tunnel_renderer.store_error(format!(
            "Request identity error, are you discovering from the right environment?\n  {err}"
        ));

        return Ok(Response::builder()
            .status(StatusCode::UNAUTHORIZED)
            .body(Body::default())
            .unwrap());
    }

    let mut result = match inner.client.execute(request).await {
        Ok(result) => result,
        Err(err) => {
            error!("Failed to proxy request: {}", err);

            inner.tunnel_renderer.store_error(err);

            return Ok(Response::builder()
                .status(StatusCode::BAD_GATEWAY)
                .body(Body::default())
                .unwrap());
        }
    };
    info!("Request proxied with status {}", result.status());
    inner.tunnel_renderer.clear_error();

    let mut response = Response::builder().status(result.status());
    if let Some(headers) = response.headers_mut() {
        std::mem::swap(headers, result.headers_mut())
    };

    Ok(response
        .body(Body::wrap_stream(result.bytes_stream()))
        .unwrap())
}
