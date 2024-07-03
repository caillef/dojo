//! Client implementation for the gRPC service.
use std::num::ParseIntError;

use futures_util::stream::MapOk;
use futures_util::{Stream, StreamExt, TryStreamExt};
use starknet::core::types::{Felt, FromStrError, StateDiff, StateUpdate};
use starknet_crypto::FieldElement;
use tonic::transport::Endpoint;
use tonic::transport::{Certificate, ClientTlsConfig};

use crate::proto::world::{
    world_client, MetadataRequest, RetrieveEntitiesRequest, RetrieveEntitiesResponse,
    RetrieveEventsRequest, RetrieveEventsResponse, SubscribeEntitiesRequest,
    SubscribeEntityResponse, SubscribeEventsRequest, SubscribeEventsResponse,
    SubscribeModelsRequest, SubscribeModelsResponse,
};
use crate::types::schema::{self, Entity, SchemaError};
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
    Transport(tonic::transport::Error),
    #[error(transparent)]
    Schema(#[from] schema::SchemaError),
}

/// A lightweight wrapper around the grpc client.
pub struct WorldClient {
    _world_address: Felt,
    #[cfg(not(target_arch = "wasm32"))]
    inner: world_client::WorldClient<tonic::transport::Channel>,
    #[cfg(target_arch = "wasm32")]
    inner: world_client::WorldClient<tonic_web_wasm_client::Client>,
}

impl WorldClient {
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn new(dst: String, _world_address: Felt) -> Result<Self, Error>
    {
        let endpoint = Endpoint::from_shared(dst).unwrap();
        let uri = endpoint.uri().clone();
        // Localhost
        if uri.to_string().starts_with("http://") {
            return Ok(Self {
                _world_address,
                inner: world_client::WorldClient::connect(endpoint).await.map_err(Error::Transport)?,
            })
        }
        let cert = "-----BEGIN CERTIFICATE-----\nMIIFZjCCBE6gAwIBAgIQEbpB21mrc3YS8iQMYdzg4zANBgkqhkiG9w0BAQsFADBG\nMQswCQYDVQQGEwJVUzEiMCAGA1UEChMZR29vZ2xlIFRydXN0IFNlcnZpY2VzIExM\nQzETMBEGA1UEAxMKR1RTIENBIDFENDAeFw0yNDA1MTYxMDMzMDJaFw0yNDA4MTQx\nMTI1MTVaMBsxGTAXBgNVBAMTEGFwaS5jYXJ0cmlkZ2UuZ2cwggEiMA0GCSqGSIb3\nDQEBAQUAA4IBDwAwggEKAoIBAQClRGB6UeB1AD+KFBhaqIlENu/hUJFftEyX45di\n6FDHfKnPhrKxmSaHRwBSWWRNZlrGbBzn+KkgVABuy+HWxvxjXpygfoNaKflHx6Qt\niSBhVrMoQTKg8ArCFMDX5cDf+9khm/v/IMSjy9kg1EoW0pDHwub2PSD8jMx/ZNMq\nCoMk/1y520w/W5/7Ej9YGG8wy28kUHLOblDw2FK6ddxYvFykcZI9e/J68F7QVndx\n/Ztf0IOJASHJIUJLnzAvkGbghKn99XIzSK8V5B3NUD4glAa6jSn42nzkNfmOBqFO\n89a5QR0aWFIjUkTMuKq0aiLedESdjE8wOavHsmRmgBRIaUKzAgMBAAGjggJ5MIIC\ndTAOBgNVHQ8BAf8EBAMCBaAwEwYDVR0lBAwwCgYIKwYBBQUHAwEwDAYDVR0TAQH/\nBAIwADAdBgNVHQ4EFgQUgVYw48mXFtjinK8S0oXPZg/FBXAwHwYDVR0jBBgwFoAU\nJeIYDrJXkZQq5dRdhpCD3lOzuJIweAYIKwYBBQUHAQEEbDBqMDUGCCsGAQUFBzAB\nhilodHRwOi8vb2NzcC5wa2kuZ29vZy9zL2d0czFkNC9DNEVZTXU2QlBkczAxBggr\nBgEFBQcwAoYlaHR0cDovL3BraS5nb29nL3JlcG8vY2VydHMvZ3RzMWQ0LmRlcjAb\nBgNVHREEFDASghBhcGkuY2FydHJpZGdlLmdnMCEGA1UdIAQaMBgwCAYGZ4EMAQIB\nMAwGCisGAQQB1nkCBQMwPAYDVR0fBDUwMzAxoC+gLYYraHR0cDovL2NybHMucGtp\nLmdvb2cvZ3RzMWQ0L2ttbGNpNnNvdTZBLmNybDCCAQYGCisGAQQB1nkCBAIEgfcE\ngfQA8gB3ABmYEHEJ8NZSLjCA0p4/ZLuDbijM+Q9Sju7fzko/FrTKAAABj4EtVdwA\nAAQDAEgwRgIhALO97Nz8MjoflZJsDFNoyGnj8pdML7zyaX+bNKhqlRS5AiEA/hdN\nzVwS0oqIeLwk+y+b6/BzdgHxVO4d/jEWjli2NvsAdwB2/4g/Crb7lVHCYcz1h7o0\ntKTNuyncaEIKn+ZnTFo6dAAAAY+BLVXuAAAEAwBIMEYCIQDX9Jk0iryHgLJz5qxw\ndf4z65QEH0Cr5W2I2EHqTm6WlgIhALVTGPb6rf0e993Q3oIr2kAHB0zqbQQ+NrcO\nYMeya5vLMA0GCSqGSIb3DQEBCwUAA4IBAQAB9TMaHnzul5XTmc6A+koLXlX9tQvf\norDSJ1+tM5X1a4vbXo0H4wIt1iXhzlsvonALq9aPV/32Eu6t1tOwn+CvlwtSoJIV\ndRYJRUEoJBrLxOk4M5yuQHRvJJdCb672IoXcOeCqDE670YwlwHBcQqbI5R3h/JUB\n9BcDs8TkUmPr2Eb9j2z5potSaHExnEdtH02YSGybXakN50CLcXgNbBigshe9Xk3k\nzIfEfyEQB+8vRiI6wigew6+641oS7TsjZvMHaw7cP0sOMaljOVkBR1Bc9LfliR8t\nPINAgTLCrVOO4Vk8ZzknJu7KV3aQmDBEEGeDBkT+buvyStS/1BQ83KiD\n-----END CERTIFICATE-----\n-----BEGIN CERTIFICATE-----\nMIIFjDCCA3SgAwIBAgINAgCOsgIzNmWLZM3bmzANBgkqhkiG9w0BAQsFADBHMQsw\nCQYDVQQGEwJVUzEiMCAGA1UEChMZR29vZ2xlIFRydXN0IFNlcnZpY2VzIExMQzEU\nMBIGA1UEAxMLR1RTIFJvb3QgUjEwHhcNMjAwODEzMDAwMDQyWhcNMjcwOTMwMDAw\nMDQyWjBGMQswCQYDVQQGEwJVUzEiMCAGA1UEChMZR29vZ2xlIFRydXN0IFNlcnZp\nY2VzIExMQzETMBEGA1UEAxMKR1RTIENBIDFENDCCASIwDQYJKoZIhvcNAQEBBQAD\nggEPADCCAQoCggEBAKvAqqPCE27l0w9zC8dTPIE89bA+xTmDaG7y7VfQ4c+mOWhl\nUebUQpK0yv2r678RJExK0HWDjeq+nLIHN1Em5j6rARZixmyRSjhIR0KOQPGBMUld\nsaztIIJ7O0g/82qj/vGDl//3t4tTqxiRhLQnTLXJdeB+2DhkdU6IIgx6wN7E5NcU\nH3Rcsejcqj8p5Sj19vBm6i1FhqLGymhMFroWVUGO3xtIH91dsgy4eFKcfKVLWK3o\n2190Q0Lm/SiKmLbRJ5Au4y1euFJm2JM9eB84Fkqa3ivrXWUeVtye0CQdKvsY2Fka\nzvxtxvusLJzLWYHk55zcRAacDA2SeEtBbQfD1qsCAwEAAaOCAXYwggFyMA4GA1Ud\nDwEB/wQEAwIBhjAdBgNVHSUEFjAUBggrBgEFBQcDAQYIKwYBBQUHAwIwEgYDVR0T\nAQH/BAgwBgEB/wIBADAdBgNVHQ4EFgQUJeIYDrJXkZQq5dRdhpCD3lOzuJIwHwYD\nVR0jBBgwFoAU5K8rJnEaK0gnhS9SZizv8IkTcT4waAYIKwYBBQUHAQEEXDBaMCYG\nCCsGAQUFBzABhhpodHRwOi8vb2NzcC5wa2kuZ29vZy9ndHNyMTAwBggrBgEFBQcw\nAoYkaHR0cDovL3BraS5nb29nL3JlcG8vY2VydHMvZ3RzcjEuZGVyMDQGA1UdHwQt\nMCswKaAnoCWGI2h0dHA6Ly9jcmwucGtpLmdvb2cvZ3RzcjEvZ3RzcjEuY3JsME0G\nA1UdIARGMEQwCAYGZ4EMAQIBMDgGCisGAQQB1nkCBQMwKjAoBggrBgEFBQcCARYc\naHR0cHM6Ly9wa2kuZ29vZy9yZXBvc2l0b3J5LzANBgkqhkiG9w0BAQsFAAOCAgEA\nIVToy24jwXUr0rAPc924vuSVbKQuYw3nLflLfLh5AYWEeVl/Du18QAWUMdcJ6o/q\nFZbhXkBH0PNcw97thaf2BeoDYY9Ck/b+UGluhx06zd4EBf7H9P84nnrwpR+4GBDZ\nK+Xh3I0tqJy2rgOqNDflr5IMQ8ZTWA3yltakzSBKZ6XpF0PpqyCRvp/NCGv2KX2T\nuPCJvscp1/m2pVTtyBjYPRQ+QuCQGAJKjtN7R5DFrfTqMWvYgVlpCJBkwlu7+7KY\n3cTIfzE7cmALskMKNLuDz+RzCcsYTsVaU7Vp3xL60OYhqFkuAOOxDZ6pHOj9+OJm\nYgPmOT4X3+7L51fXJyRH9KfLRP6nT31D5nmsGAOgZ26/8T9hsBW1uo9ju5fZLZXV\nVS5H0HyIBMEKyGMIPhFWrlt/hFS28N1zaKI0ZBGD3gYgDLbiDT9fGXstpk+Fmc4o\nlVlWPzXe81vdoEnFbr5M272HdgJWo+WhT9BYM0Ji+wdVmnRffXgloEoluTNcWzc4\n1dFpgJu8fF3LG0gl2ibSYiCi9a6hvU0TppjJyIWXhkJTcMJlPrWx1VytEUGrX2l0\nJDwRjW/656r0KVB02xHRKvm2ZKI03TglLIpmVCK3kBKkKNpBNkFt8rhafcCKOb9J\nx/9tpNFlQTl7B39rJlJWkR17QnZqVptFePFORoZmFzM=\n-----END CERTIFICATE-----\n-----BEGIN CERTIFICATE-----\nMIIFYjCCBEqgAwIBAgIQd70NbNs2+RrqIQ/E8FjTDTANBgkqhkiG9w0BAQsFADBX\nMQswCQYDVQQGEwJCRTEZMBcGA1UEChMQR2xvYmFsU2lnbiBudi1zYTEQMA4GA1UE\nCxMHUm9vdCBDQTEbMBkGA1UEAxMSR2xvYmFsU2lnbiBSb290IENBMB4XDTIwMDYx\nOTAwMDA0MloXDTI4MDEyODAwMDA0MlowRzELMAkGA1UEBhMCVVMxIjAgBgNVBAoT\nGUdvb2dsZSBUcnVzdCBTZXJ2aWNlcyBMTEMxFDASBgNVBAMTC0dUUyBSb290IFIx\nMIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAthECix7joXebO9y/lD63\nladAPKH9gvl9MgaCcfb2jH/76Nu8ai6Xl6OMS/kr9rH5zoQdsfnFl97vufKj6bwS\niV6nqlKr+CMny6SxnGPb15l+8Ape62im9MZaRw1NEDPjTrETo8gYbEvs/AmQ351k\nKSUjB6G00j0uYODP0gmHu81I8E3CwnqIiru6z1kZ1q+PsAewnjHxgsHA3y6mbWwZ\nDrXYfiYaRQM9sHmklCitD38m5agI/pboPGiUU+6DOogrFZYJsuB6jC511pzrp1Zk\nj5ZPaK49l8KEj8C8QMALXL32h7M1bKwYUH+E4EzNktMg6TO8UpmvMrUpsyUqtEj5\ncuHKZPfmghCN6J3Cioj6OGaK/GP5Afl4/Xtcd/p2h/rs37EOeZVXtL0m79YB0esW\nCruOC7XFxYpVq9Os6pFLKcwZpDIlTirxZUTQAs6qzkm06p98g7BAe+dDq6dso499\niYH6TKX/1Y7DzkvgtdizjkXPdsDtQCv9Uw+wp9U7DbGKogPeMa3Md+pvez7W35Ei\nEua++tgy/BBjFFFy3l3WFpO9KWgz7zpm7AeKJt8T11dleCfeXkkUAKIAf5qoIbap\nsZWwpbkNFhHax2xIPEDgfg1azVY80ZcFuctL7TlLnMQ/0lUTbiSw1nH69MG6zO0b\n9f6BQdgAmD06yK56mDcYBZUCAwEAAaOCATgwggE0MA4GA1UdDwEB/wQEAwIBhjAP\nBgNVHRMBAf8EBTADAQH/MB0GA1UdDgQWBBTkrysmcRorSCeFL1JmLO/wiRNxPjAf\nBgNVHSMEGDAWgBRge2YaRQ2XyolQL30EzTSo//z9SzBgBggrBgEFBQcBAQRUMFIw\nJQYIKwYBBQUHMAGGGWh0dHA6Ly9vY3NwLnBraS5nb29nL2dzcjEwKQYIKwYBBQUH\nMAKGHWh0dHA6Ly9wa2kuZ29vZy9nc3IxL2dzcjEuY3J0MDIGA1UdHwQrMCkwJ6Al\noCOGIWh0dHA6Ly9jcmwucGtpLmdvb2cvZ3NyMS9nc3IxLmNybDA7BgNVHSAENDAy\nMAgGBmeBDAECATAIBgZngQwBAgIwDQYLKwYBBAHWeQIFAwIwDQYLKwYBBAHWeQIF\nAwMwDQYJKoZIhvcNAQELBQADggEBADSkHrEoo9C0dhemMXoh6dFSPsjbdBZBiLg9\nNR3t5P+T4Vxfq7vqfM/b5A3Ri1fyJm9bvhdGaJQ3b2t6yMAYN/olUazsaL+yyEn9\nWprKASOshIArAoyZl+tJaox118fessmXn1hIVw41oeQa1v1vg4Fv74zPl6/AhSrw\n9U5pCZEt4Wi4wStz6dTZ/CLANx8LZh1J7QJVj2fhMtfTJr9w4z30Z209fOU0iOMy\n+qduBmpvvYuR7hZL6Dupszfnw0Skfths18dG9ZKb59UhvmaSGZRVbNQpsg3BZlvi\nd0lIKO2d1xozclOzgjXPYovJJIultzkMu34qQb9Sz/yilrbCgj8=\n-----END CERTIFICATE-----\n";
        let config = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(&cert))
            .domain_name("api.cartridge.gg");

        let channel = endpoint
            .tls_config(config)
            .unwrap()
            .connect()
            .await
            .unwrap();

        Ok(Self {
            _world_address,
            inner: world_client::WorldClient::with_origin(channel, uri),
         })
    }

    // we make this function async so that we can keep the function signature similar
    #[cfg(target_arch = "wasm32")]
    pub async fn new(endpoint: String, _world_address: Felt) -> Result<Self, Error> {
        Ok(Self {
            _world_address,
            inner: world_client::WorldClient::new(tonic_web_wasm_client::Client::new(endpoint)),
        })
    }

    /// Retrieve the metadata of the World.
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
            None => Entity { hashed_keys: Felt::ZERO, models: vec![] },
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
            None => Entity { hashed_keys: Felt::ZERO, models: vec![] },
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
            Some(event) => event.into(),
            None => Event { keys: vec![], data: vec![], transaction_hash: Felt::ZERO },
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
        block_hash: Felt::ZERO,
        new_root: Felt::ZERO,
        old_root: Felt::ZERO,
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
