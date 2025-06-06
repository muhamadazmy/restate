// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;
use http::{Method, Request, Response, StatusCode, header};
use http_body_util::Full;
use serde::Serialize;

use restate_types::schema::service::ServiceMetadataResolver;

use super::{APPLICATION_JSON, Handler};
use crate::handler::error::HandlerError;

#[derive(Debug, Serialize)]
#[cfg_attr(test, derive(serde::Deserialize))]
#[serde(rename_all = "camelCase")]
pub(crate) struct HealthResponse {
    services: Vec<String>,
}

impl<Schemas, Dispatcher> Handler<Schemas, Dispatcher>
where
    Schemas: ServiceMetadataResolver + Send + Sync + 'static,
{
    pub(crate) fn handle_health<B: http_body::Body>(
        &mut self,
        req: Request<B>,
    ) -> Result<Response<Full<Bytes>>, HandlerError> {
        if req.method() != Method::GET {
            return Err(HandlerError::MethodNotAllowed);
        }
        let response = HealthResponse {
            services: self
                .schemas
                .pinned()
                .list_services()
                .into_iter()
                .map(|c| c.name)
                .collect(),
        };
        Ok(Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, APPLICATION_JSON)
            .body(Full::new(
                serde_json::to_vec(&response)
                    .expect("Serializing the HealthResponse must not fail")
                    .into(),
            ))
            .unwrap())
    }
}
