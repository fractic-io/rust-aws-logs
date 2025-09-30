use std::{collections::HashMap, sync::Arc};

use aws_sdk_cloudwatchlogs::{
    error::SdkError,
    operation::{
        create_log_stream::CreateLogStreamError, describe_log_streams::DescribeLogStreamsError,
    },
    types::InputLogEvent,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{
    errors::{CloudWatchCalloutError, CloudWatchInvalidOperation, LogGroupNotFound},
    LogsCtxView,
};
use fractic_server_error::{CriticalError, ServerError};

pub mod backend;
use backend::LogsBackend;

/// Configuration.
/// --------------------------------------------------------------------------

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

/// Extensions.
/// --------------------------------------------------------------------------

/// Implement on T to enable EMF logging.
pub trait MetricExport {
    fn metric_namespace(&self) -> &str;
    fn metric_dimensions(&self) -> HashMap<String, String>;
    fn metric_name(&self) -> &str;
    fn metric_unit(&self) -> &str;
    fn metric_value(&self) -> f64;
}

/// Implementation.
/// --------------------------------------------------------------------------

pub struct LogsUtil<T: Serialize + Send + Sync> {
    pub backend: Arc<dyn LogsBackend>,
    pub config: LogConfig<T>,
    stream_name: String,
}

impl<T: Serialize + Send + Sync> LogsUtil<T> {
    pub async fn new(ctx: &dyn LogsCtxView, config: LogConfig<T>) -> Result<Self, ServerError> {
        let backend = ctx.logs_backend().await?;
        let stream_name = ensure_stream(&*backend, &config).await?;
        Ok(Self {
            backend,
            config,
            stream_name,
        })
    }

    pub async fn log(&self, item: T) -> Result<(), ServerError> {
        let message = serde_json::to_string(&item).map_err(|e| {
            CloudWatchInvalidOperation::with_debug("failed to serialize log item", &e)
        })?;
        let event = InputLogEvent::builder()
            .message(message)
            .timestamp(chrono::Utc::now().timestamp_millis())
            .build()
            .map_err(|e| CloudWatchInvalidOperation::with_debug("failed to build log event", &e))?;

        self.backend
            .put_log_events(
                self.config.log_group.clone(),
                self.stream_name.clone(),
                vec![event],
            )
            .await
            .map_err(|e| CloudWatchCalloutError::with_debug("failed to put log events", &e))?;
        Ok(())
    }
}

impl<T: Serialize + Send + Sync + MetricExport> LogsUtil<T> {
    pub async fn log_emf(&self, item: T) -> Result<(), ServerError> {
        #[derive(Serialize)]
        struct EmfRoot<P> {
            #[serde(rename = "_aws")]
            aws: EmfAws,
            #[serde(flatten)]
            payload: P,
        }
        #[derive(Serialize)]
        #[allow(non_snake_case)]
        struct EmfAws {
            Timestamp: i64,
            CloudWatchMetrics: Vec<EmfMetricDirective>,
        }
        #[derive(Serialize)]
        #[allow(non_snake_case)]
        struct EmfMetricDirective {
            Namespace: String,
            Dimensions: Vec<Vec<String>>,
            Metrics: Vec<EmfMetricDefinition>,
        }
        #[derive(Serialize)]
        #[allow(non_snake_case)]
        struct EmfMetricDefinition {
            Name: String,
            Unit: String,
        }

        let ts = chrono::Utc::now().timestamp_millis();
        let dims = item.metric_dimensions();
        let dims_keys: Vec<String> = dims.keys().cloned().collect();

        let mut payload = serde_json::to_value(&item).map_err(|e| {
            CloudWatchInvalidOperation::with_debug("failed to serialize EMF payload", &e)
        })?;
        if let serde_json::Value::Object(ref mut map) = payload {
            for (k, v) in &dims {
                map.insert(k.clone(), json!(v));
            }
            map.insert(item.metric_name().to_string(), json!(item.metric_value()));
        }

        let emf = EmfRoot {
            aws: EmfAws {
                Timestamp: ts,
                CloudWatchMetrics: vec![EmfMetricDirective {
                    Namespace: item.metric_namespace().to_string(),
                    Dimensions: vec![dims_keys],
                    Metrics: vec![EmfMetricDefinition {
                        Name: item.metric_name().to_string(),
                        Unit: item.metric_unit().to_string(),
                    }],
                }],
            },
            payload,
        };

        let message = serde_json::to_string(&emf).map_err(|e| {
            CloudWatchInvalidOperation::with_debug("failed to serialize EMF log", &e)
        })?;
        let event = InputLogEvent::builder()
            .message(message)
            .timestamp(ts)
            .build()
            .map_err(|e| {
                CloudWatchInvalidOperation::with_debug("failed to build EMF log event", &e)
            })?;

        self.backend
            .put_log_events(
                self.config.log_group.clone(),
                self.stream_name.clone(),
                vec![event],
            )
            .await
            .map_err(|e| CloudWatchCalloutError::with_debug("failed to put EMF log", &e))?;
        Ok(())
    }
}

// Helpers.
// ---------------------------------------------------------------------------

/// Checks the log group exists, and creates a log stream if not already
/// present. Returns the log stream name.
async fn ensure_stream<T>(
    backend: &dyn LogsBackend,
    config: &LogConfig<T>,
) -> Result<String, ServerError> {
    let stream_name = match config.stream_naming {
        StreamNaming::Date => chrono::Utc::now().format("%Y-%m-%d").to_string(),
        StreamNaming::Uuid => uuid::Uuid::new_v4().to_string(),
    };

    let search_result = backend
        .describe_log_stream(config.log_group.clone(), stream_name.clone())
        .await
        .map_err(|e| match e {
            SdkError::ServiceError(se) => match se.err() {
                DescribeLogStreamsError::ResourceNotFoundException(_) => {
                    LogGroupNotFound::new(&config.log_group)
                }
                _ => CloudWatchCalloutError::with_debug("failed to describe log stream", &se.err()),
            },
            other => CloudWatchCalloutError::with_debug("failed to describe log stream", &other),
        })?;

    let exists = search_result
        .log_streams()
        .iter()
        .any(|s| s.log_stream_name() == Some(&stream_name));
    if !exists {
        match backend
            .create_log_stream(config.log_group.clone(), stream_name.clone())
            .await
        {
            Ok(_) => Ok(()),
            Err(SdkError::ServiceError(se)) => match se.err() {
                CreateLogStreamError::ResourceNotFoundException(_) => {
                    Err(CriticalError::new(&format!(
                        "log group '{}' not found, despite checking existence above",
                        config.log_group
                    )))
                }
                CreateLogStreamError::ResourceAlreadyExistsException(_) => {
                    // Benign race; stream is there now.
                    Ok(())
                }
                _ => Err(CloudWatchCalloutError::with_debug(
                    "failed to create log stream",
                    &se.err(),
                )),
            },
            Err(other) => Err(CloudWatchCalloutError::with_debug(
                "failed to create log stream",
                &other,
            )),
        }?;
    }

    Ok(stream_name)
}
