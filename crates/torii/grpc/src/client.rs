//! Client implementation for the gRPC service.
use std::io::{Read, Write};
use std::net::TcpStream;
use std::num::ParseIntError;
use std::str::Utf8Error;

use futures_util::stream::MapOk;
use futures_util::{Stream, StreamExt, TryStreamExt};
use openssl::error::ErrorStack;
use openssl::ssl::{SslConnector, SslMethod};
use starknet::core::types::{FromStrError, StateDiff, StateUpdate};
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Error as TonicTransportError};
use starknet_crypto::FieldElement;

use crate::proto::world::{
    world_client, MetadataRequest, RetrieveEntitiesRequest, RetrieveEntitiesResponse,
    RetrieveEventsRequest, RetrieveEventsResponse, SubscribeEntitiesRequest,
    SubscribeEntityResponse, SubscribeEventsRequest, SubscribeEventsResponse,
    SubscribeModelsRequest, SubscribeModelsResponse,
};
use crate::types::schema::{Entity, SchemaError};
use crate::types::{EntityKeysClause, Event, EventQuery, KeysClause, ModelKeysClause, Query};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Grpc(tonic::Status),
    #[error(transparent)]
    ParseStr(FromStrError),
    #[error(transparent)]
    ParseInt(ParseIntError),
    #[cfg(not(target_arch = "wasm32"))]
    #[error(transparent)]
    Transport(TonicTransportError),
    #[error(transparent)]
    Schema(SchemaError),
    #[error("Endpoint error: {0}")]
    Endpoint(String),
    #[error("Invalid URI: {0}")]
    InvalidUri(String),
    #[error(transparent)]
    Ssl(ErrorStack),
    #[error(transparent)]
    Tls(openssl::ssl::HandshakeError<TcpStream>),
    #[error(transparent)]
    Tcp(std::io::Error),
    #[error(transparent)]
    Io(std::io::Error),
    #[error(transparent)]
    Utf8(Utf8Error),
    #[error("TLS config error: {0}")]
    TlsConfig(String),
}

/// A lightweight wrapper around the grpc client.
pub struct WorldClient {
    _world_address: FieldElement,
    #[cfg(not(target_arch = "wasm32"))]
    inner: world_client::WorldClient<tonic::transport::Channel>,
    #[cfg(target_arch = "wasm32")]
    inner: world_client::WorldClient<tonic_web_wasm_client::Client>,
}

impl WorldClient {
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn new(dst: String, _world_address: FieldElement) -> Result<Self, Error> {
        let endpoint = Endpoint::from_shared(dst).map_err(|e| Error::Endpoint(e.to_string()))?;
        let uri = endpoint.uri().clone();

        // Localhost
        if uri.to_string().starts_with("http://") {
            return Ok(Self {
                _world_address,
                inner: world_client::WorldClient::connect(endpoint)
                    .await
                    .map_err(Error::Transport)?,
            });
        }

        let host = uri.host().ok_or_else(|| Error::InvalidUri("Missing host".into()))?;
        let connector = SslConnector::builder(SslMethod::tls()).map_err(Error::Ssl)?.build();
        let tcp_stream = TcpStream::connect(&(host.to_owned() + ":443")).map_err(Error::Tcp)?;
        let mut stream = connector.connect(&host, tcp_stream).map_err(Error::Tls)?;

        stream.write_all(b"GET / HTTP/1.0\r\n\r\n").map_err(Error::Io)?;
        let mut response = vec![];
        stream.read_to_end(&mut response).map_err(Error::Io)?;

        let certs = stream.ssl().peer_cert_chain().ok_or_else(|| Error::Ssl(ErrorStack::get()))?;
        let mut certs_str = String::new();
        for cert in certs {
            let pem = cert.to_pem().map_err(Error::Ssl)?;
            certs_str.push_str(std::str::from_utf8(&pem).map_err(Error::Utf8)?);
        }

        let config = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(&certs_str))
            .domain_name(host);

        let channel = endpoint
            .tls_config(config)
            .map_err(|e| Error::TlsConfig(e.to_string()))?
            .connect()
            .await
            .map_err(Error::Transport)?;

        Ok(Self { _world_address, inner: world_client::WorldClient::with_origin(channel, uri) })
    }

    #[cfg(target_arch = "wasm32")]
    pub async fn new(endpoint: String, _world_address: FieldElement) -> Result<Self, Error> {
        Ok(Self {
            _world_address,
            inner: world_client::WorldClient::new(tonic_web_wasm_client::Client::new(endpoint)),
        })
    }

    pub async fn metadata(&mut self) -> Result<dojo_types::WorldMetadata, Error> {
        self.inner
            .world_metadata(MetadataRequest {})
            .await
            .map_err(Error::Grpc)
            .and_then(|res| {
                res.into_inner().metadata.ok_or(Error::Schema(SchemaError::MissingExpectedData))
            })
            .and_then(|metadata| metadata.try_into().map_err(Error::ParseStr))
    }

    pub async fn retrieve_entities(
        &mut self,
        query: Query,
    ) -> Result<RetrieveEntitiesResponse, Error> {
        let request = RetrieveEntitiesRequest { query: Some(query.into()) };
        self.inner.retrieve_entities(request).await.map_err(Error::Grpc).map(|res| res.into_inner())
    }

    pub async fn retrieve_event_messages(
        &mut self,
        query: Query,
    ) -> Result<RetrieveEntitiesResponse, Error> {
        let request = RetrieveEntitiesRequest { query: Some(query.into()) };
        self.inner
            .retrieve_event_messages(request)
            .await
            .map_err(Error::Grpc)
            .map(|res| res.into_inner())
    }

    pub async fn retrieve_events(
        &mut self,
        query: EventQuery,
    ) -> Result<RetrieveEventsResponse, Error> {
        let request = RetrieveEventsRequest { query: Some(query.into()) };
        self.inner.retrieve_events(request).await.map_err(Error::Grpc).map(|res| res.into_inner())
    }

    /// Subscribe to entities updates of a World.
    pub async fn subscribe_entities(
        &mut self,
        clause: Option<EntityKeysClause>,
    ) -> Result<EntityUpdateStreaming, Error> {
        let clause = clause.map(|c| c.into());
        let stream = self
            .inner
            .subscribe_entities(SubscribeEntitiesRequest { clause })
            .await
            .map_err(Error::Grpc)
            .map(|res| res.into_inner())?;

        Ok(EntityUpdateStreaming(stream.map_ok(Box::new(|res| match res.entity {
            Some(entity) => entity.try_into().expect("must able to serialize"),
            None => Entity { hashed_keys: FieldElement::ZERO, models: vec![] },
        }))))
    }

    /// Subscribe to event messages of a World.
    pub async fn subscribe_event_messages(
        &mut self,
        clause: Option<EntityKeysClause>,
    ) -> Result<EntityUpdateStreaming, Error> {
        let clause = clause.map(|c| c.into());
        let stream = self
            .inner
            .subscribe_event_messages(SubscribeEntitiesRequest { clause })
            .await
            .map_err(Error::Grpc)
            .map(|res| res.into_inner())?;

        Ok(EntityUpdateStreaming(stream.map_ok(Box::new(|res| match res.entity {
            Some(entity) => entity.try_into().expect("must able to serialize"),
            None => Entity { hashed_keys: FieldElement::ZERO, models: vec![] },
        }))))
    }

    /// Subscribe to the events of a World.
    pub async fn subscribe_events(
        &mut self,
        keys: Option<KeysClause>,
    ) -> Result<EventUpdateStreaming, Error> {
        let keys = keys.map(|c| c.into());

        let stream = self
            .inner
            .subscribe_events(SubscribeEventsRequest { keys })
            .await
            .map_err(Error::Grpc)
            .map(|res| res.into_inner())?;

        Ok(EventUpdateStreaming(stream.map_ok(Box::new(|res| match res.event {
            Some(event) => event.try_into().expect("must able to serialize"),
            None => Event { keys: vec![], data: vec![], transaction_hash: FieldElement::ZERO },
        }))))
    }

    /// Subscribe to the model diff for a set of models of a World.
    pub async fn subscribe_model_diffs(
        &mut self,
        models_keys: Vec<ModelKeysClause>,
    ) -> Result<ModelDiffsStreaming, Error> {
        let stream = self
            .inner
            .subscribe_models(SubscribeModelsRequest {
                models_keys: models_keys.into_iter().map(|e| e.into()).collect(),
            })
            .await
            .map_err(Error::Grpc)
            .map(|res| res.into_inner())?;

        Ok(ModelDiffsStreaming(stream.map_ok(Box::new(|res| match res.model_update {
            Some(update) => {
                TryInto::<StateUpdate>::try_into(update).expect("must able to serialize")
            }
            None => empty_state_update(),
        }))))
    }
}

type ModelDiffMappedStream = MapOk<
    tonic::Streaming<SubscribeModelsResponse>,
    Box<dyn Fn(SubscribeModelsResponse) -> StateUpdate + Send>,
>;

pub struct ModelDiffsStreaming(ModelDiffMappedStream);

impl Stream for ModelDiffsStreaming {
    type Item = <ModelDiffMappedStream as Stream>::Item;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.0.poll_next_unpin(cx)
    }
}

type EntityMappedStream = MapOk<
    tonic::Streaming<SubscribeEntityResponse>,
    Box<dyn Fn(SubscribeEntityResponse) -> Entity + Send>,
>;

pub struct EntityUpdateStreaming(EntityMappedStream);

impl Stream for EntityUpdateStreaming {
    type Item = <EntityMappedStream as Stream>::Item;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.0.poll_next_unpin(cx)
    }
}

type EventMappedStream = MapOk<
    tonic::Streaming<SubscribeEventsResponse>,
    Box<dyn Fn(SubscribeEventsResponse) -> Event + Send>,
>;

pub struct EventUpdateStreaming(EventMappedStream);

impl Stream for EventUpdateStreaming {
    type Item = <EventMappedStream as Stream>::Item;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.0.poll_next_unpin(cx)
    }
}

fn empty_state_update() -> StateUpdate {
    StateUpdate {
        block_hash: FieldElement::ZERO,
        new_root: FieldElement::ZERO,
        old_root: FieldElement::ZERO,
        state_diff: StateDiff {
            declared_classes: vec![],
            deployed_contracts: vec![],
            deprecated_declared_classes: vec![],
            nonces: vec![],
            replaced_classes: vec![],
            storage_diffs: vec![],
        },
    }
}
