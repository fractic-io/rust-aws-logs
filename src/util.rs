use std::sync::Arc;

use aws_sdk_cloudwatchlogs::{
    error::SdkError,
    operation::{
        create_log_stream::CreateLogStreamError,
        describe_log_streams::DescribeLogStreamsError,
        put_log_events::PutLogEventsError,
    },
    types::InputLogEvent,
};
use backend::LogsBackend;
use fractic_server_error::ServerError;
use serde::{Deserialize, Serialize};

use crate::{
    errors::{CloudWatchCalloutError, CloudWatchInvalidOperation, LogGroupNotFound},
    LogsCtxView,
};

pub mod backend;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum StreamNaming {
    Date,
    Uuid,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogConfig<T> {
    pub log_group: String,
    pub stream_naming: StreamNaming,
    #[serde(skip)]
    pub _phantom: std::marker::PhantomData<T>,
}

impl<T> LogConfig<T> {
    pub fn new(log_group: impl Into<String>, stream_naming: StreamNaming) -> Self {
        Self {
            log_group: log_group.into(),
            stream_naming,
            _phantom: std::marker::PhantomData,
        }
    }
}

pub struct LogsUtil<T: Serialize + Send + Sync> {
    pub backend: Arc<dyn LogsBackend>,
    pub config: LogConfig<T>,

    /// For UUID-based stream naming, reuse the same stream for the lifetime of
    /// the util.
    uuid_stream_name: Option<String>,
}

impl<T: Serialize + Send + Sync> LogsUtil<T> {
    pub async fn new(ctx: &dyn LogsCtxView, config: LogConfig<T>) -> Result<Self, ServerError> {
        Ok(Self {
            backend: ctx.logs_backend().await?,
            config,
            uuid_stream_name: None,
        })
    }

    fn compute_stream_name(&self) -> String {
        match self.config.stream_naming {
            StreamNaming::Date => chrono::Utc::now().format("%Y-%m-%d").to_string(),
            StreamNaming::Uuid => self
                .uuid_stream_name
                .clone()
                .unwrap_or_else(|| format!("{}", uuid::Uuid::new_v4())),
        }
    }

    async fn ensure_stream(&mut self, stream_name: &str) -> Result<(), ServerError> {
        let desc = self
            .backend
            .describe_log_stream(
                self.config.log_group.clone(),
                stream_name.to_string(),
            )
            .await;

        match desc {
            Ok(output) => {
                let exists = output
                    .log_streams()
                    .iter()
                    .any(|s| s.log_stream_name() == Some(stream_name));
                if exists {
                    return Ok(());
                }

                // Stream missing; attempt to create it.
                self
                    .backend
                    .create_log_stream(
                        self.config.log_group.clone(),
                        stream_name.to_string(),
                    )
                    .await
                    .map(|_| ())
                    .map_err(|err| match err {
                        SdkError::ServiceError(se) => match se.err() {
                            CreateLogStreamError::ResourceNotFoundException(_) =>
                                LogGroupNotFound::new(&self.config.log_group),
                            _ => CloudWatchCalloutError::with_debug(
                                "failed to create log stream",
                                &se.err().to_string(),
                            ),
                        },
                        other => CloudWatchCalloutError::with_debug(
                            "failed to create log stream",
                            &other,
                        ),
                    })
            }
            Err(err) => match err {
                SdkError::ServiceError(se) => match se.err() {
                    DescribeLogStreamsError::ResourceNotFoundException(_) =>
                        Err(LogGroupNotFound::new(&self.config.log_group)),
                    _ => Err(CloudWatchCalloutError::with_debug(
                        "failed to describe log stream",
                        &se.err().to_string(),
                    )),
                },
                other => Err(CloudWatchCalloutError::with_debug(
                    "failed to describe log stream",
                    &other,
                )),
            },
        }
    }

    async fn fetch_sequence_token(
        &self,
        stream_name: &str,
    ) -> Result<Option<String>, ServerError> {
        let out = self
            .backend
            .describe_log_stream(
                self.config.log_group.clone(),
                stream_name.to_string(),
            )
            .await
            .map_err(|e| match e {
                SdkError::ServiceError(se) => match se.err() {
                    DescribeLogStreamsError::ResourceNotFoundException(_) =>
                        LogGroupNotFound::new(&self.config.log_group),
                    _ => CloudWatchCalloutError::with_debug(
                        "failed to describe log stream",
                        &se.err().to_string(),
                    ),
                },
                other => CloudWatchCalloutError::with_debug(
                    "failed to describe log stream",
                    &other,
                ),
            })?;
        let token = out
            .log_streams()
            .first()
            .and_then(|s| s.upload_sequence_token().map(|s| s.to_string()));
        Ok(token)
    }

    pub async fn log(&mut self, item: T) -> Result<(), ServerError> {
        let stream_name = match self.config.stream_naming {
            StreamNaming::Date => self.compute_stream_name(),
            StreamNaming::Uuid => {
                if self.uuid_stream_name.is_none() {
                    self.uuid_stream_name = Some(self.compute_stream_name());
                }
                self.uuid_stream_name.clone().unwrap()
            }
        };

        self.ensure_stream(&stream_name).await?;

        let message = serde_json::to_string(&item).map_err(|e| {
            CloudWatchInvalidOperation::with_debug("failed to serialize log item", &e)
        })?;

        let event = InputLogEvent::builder()
            .message(message)
            .timestamp(chrono::Utc::now().timestamp_millis())
            .build();

        let mut sequence_token = self.fetch_sequence_token(&stream_name).await?;

        // Retry once on InvalidSequenceToken: fetch latest token and retry.
        let attempt = self
            .backend
            .put_log_events(
                self.config.log_group.clone(),
                stream_name.clone(),
                vec![event.clone()],
                sequence_token.clone(),
            )
            .await;

        match attempt {
            Ok(_) => Ok(()),
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("InvalidSequenceToken") || err_str.contains("DataAlreadyAccepted") {
                    sequence_token = self.fetch_sequence_token(&stream_name).await?;
                    self
                        .backend
                        .put_log_events(
                            self.config.log_group.clone(),
                            stream_name,
                            vec![event],
                            sequence_token,
                        )
                        .await
                        .map(|_| ())
                        .map_err(|e2| CloudWatchCalloutError::with_debug("failed to put log events (retry)", &e2))
                } else {
                    Err(CloudWatchCalloutError::with_debug("failed to put log events", &e))
                }
            }
        }
    }

    pub async fn log_emf(
        &mut self,
        item: T,
        namespace: impl Into<String>,
        dimensions: Vec<(String, String)>,
        metric_name: impl Into<String>,
        metric_unit: impl Into<String>,
        metric_value: f64,
    ) -> Result<(), ServerError> {
        #[derive(Serialize)]
        struct EmfRoot<P> {
            #[serde(rename = "_aws")]
            aws: EmfAws,
            #[serde(flatten)]
            payload: P,
        }

        #[derive(Serialize)]
        struct EmfAws {
            Timestamp: i64,
            CloudWatchMetrics: Vec<EmfMetricDirective>,
        }

        #[derive(Serialize)]
        struct EmfMetricDirective {
            Namespace: String,
            Dimensions: Vec<Vec<String>>, // list of dimension sets
            Metrics: Vec<EmfMetricDefinition>,
        }

        #[derive(Serialize)]
        struct EmfMetricDefinition {
            Name: String,
            Unit: String,
        }

        // Build EMF structure.
        let timestamp_ms = chrono::Utc::now().timestamp_millis();
        let namespace_str = namespace.into();
        let metric_def = EmfMetricDefinition {
            Name: metric_name.into(),
            Unit: metric_unit.into(),
        };
        let dims_keys: Vec<String> = dimensions.iter().map(|(k, _)| k.clone()).collect();

        // Merge item with dimension key-values. Wrap user item in an object
        // that also includes the dimensions as fields.
        use serde_json::json;
        let mut base = serde_json::to_value(&item).map_err(|e| {
            CloudWatchInvalidOperation::with_debug("failed to serialize EMF payload", &e)
        })?;
        if let serde_json::Value::Object(ref mut map) = base {
            for (k, v) in &dimensions {
                map.insert(k.clone(), json!(v));
            }
        }

        let emf = EmfRoot {
            aws: EmfAws {
                Timestamp: timestamp_ms,
                CloudWatchMetrics: vec![EmfMetricDirective {
                    Namespace: namespace_str,
                    Dimensions: vec![dims_keys],
                    Metrics: vec![metric_def],
                }],
            },
            payload: base,
        };

        // Serialize and send as a single event.
        let message = serde_json::to_string(&emf).map_err(|e| {
            CloudWatchInvalidOperation::with_debug("failed to serialize EMF log", &e)
        })?;

        let stream_name = match self.config.stream_naming {
            StreamNaming::Date => self.compute_stream_name(),
            StreamNaming::Uuid => {
                if self.uuid_stream_name.is_none() {
                    self.uuid_stream_name = Some(self.compute_stream_name());
                }
                self.uuid_stream_name.clone().unwrap()
            }
        };

        self.ensure_stream(&stream_name).await?;

        let event = InputLogEvent::builder()
            .message(message)
            .timestamp(timestamp_ms)
            .build();

        let mut sequence_token = self.fetch_sequence_token(&stream_name).await?;
        let attempt = self
            .backend
            .put_log_events(
                self.config.log_group.clone(),
                stream_name.clone(),
                vec![event.clone()],
                sequence_token.clone(),
            )
            .await;

        match attempt {
            Ok(_) => Ok(()),
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("InvalidSequenceToken") || err_str.contains("DataAlreadyAccepted") {
                    sequence_token = self.fetch_sequence_token(&stream_name).await?;
                    self
                        .backend
                        .put_log_events(
                            self.config.log_group.clone(),
                            stream_name,
                            vec![event],
                            sequence_token,
                        )
                        .await
                        .map(|_| ())
                        .map_err(|e2| CloudWatchCalloutError::with_debug("failed to put EMF log (retry)", &e2))
                } else {
                    Err(CloudWatchCalloutError::with_debug("failed to put EMF log", &e))
                }
            }
        }
    }
}
