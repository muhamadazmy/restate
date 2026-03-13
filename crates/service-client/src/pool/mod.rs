// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
pub mod conn;
pub mod tls;

use std::{
    io::{self, ErrorKind},
    net::IpAddr,
    str::FromStr,
    task::{Context, Poll},
};

use futures::future::BoxFuture;
use http::Uri;
use rustls::pki_types::{DnsName, ServerName};
use tokio::net::TcpStream;
use tower::Service;
use tracing::trace;

/// A Tower [`Service`] that establishes TCP connections to a given URI.
///
/// Extracts the host and port from the URI (defaulting to port 80 for HTTP
/// and 443 for HTTPS) and connects via [`TcpStream`].
// todo(azmy): add support for connect-timeout
#[derive(Debug, Clone, Copy)]
pub struct TcpConnector;

impl Service<Uri> for TcpConnector {
    type Response = TcpStream;
    type Error = io::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Uri) -> Self::Future {
        let fut = async move {
            let req = req.get_connection_info()?;
            trace!("connecting to {:?}:{}", req.host, req.port());

            match req.host() {
                Host::IpAddress(addr) => TcpStream::connect((*addr, req.port())).await,
                Host::DnsName(dns) => TcpStream::connect((dns.as_ref(), req.port())).await,
            }
        };

        Box::pin(fut)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Host {
    IpAddress(IpAddr),
    DnsName(DnsName<'static>),
}

impl From<Host> for ServerName<'static> {
    fn from(value: Host) -> Self {
        match value {
            Host::IpAddress(addr) => ServerName::IpAddress(addr.into()),
            Host::DnsName(dns) => ServerName::DnsName(dns),
        }
    }
}

trait IntoConnectionInfo {
    fn get_connection_info(&self) -> Result<ConnectionInfo, io::Error>;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ConnectionInfo {
    secure: bool,
    host: Host,
    port: u16,
}

impl ConnectionInfo {
    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn host(&self) -> &Host {
        &self.host
    }

    pub fn secure(&self) -> bool {
        self.secure
    }
}

impl IntoConnectionInfo for Uri {
    fn get_connection_info(&self) -> Result<ConnectionInfo, io::Error> {
        let (secure, default_port) = self
            .scheme()
            .and_then(|scheme| match scheme.as_str() {
                "http" => Some((false, 80)),
                "https" => Some((true, 443)),
                _ => None,
            })
            .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "unknown schema"))?;

        let port = self.port_u16().unwrap_or(default_port);
        let host = self
            .host()
            .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "unknown host name"))?;

        let host = match std::net::IpAddr::from_str(host) {
            Ok(addr) => Host::IpAddress(addr),
            Err(_) => {
                let dns: DnsName<'static> = host
                    .to_owned()
                    .try_into()
                    .map_err(|err| io::Error::new(ErrorKind::InvalidInput, err))?;

                Host::DnsName(dns)
            }
        };

        Ok(ConnectionInfo { secure, host, port })
    }
}

impl<T> IntoConnectionInfo for http::Request<T> {
    fn get_connection_info(&self) -> Result<ConnectionInfo, io::Error> {
        self.uri().get_connection_info()
    }
}
