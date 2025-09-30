use std::sync::Arc;

use async_trait::async_trait;
use aws_config::{BehaviorVersion, Region};
use aws_sdk_cloudwatchlogs::{
    error::SdkError,
    operation::{
        create_log_stream::{CreateLogStreamError, CreateLogStreamOutput},
        describe_log_streams::{DescribeLogStreamsError, DescribeLogStreamsOutput},
        put_log_events::{PutLogEventsError, PutLogEventsOutput},
    },
    types::InputLogEvent,
};
use fractic_context::register_ctx_singleton;

use crate::LogsCtxView;

// Underlying backend for CloudWatch Logs; kept thin to be mockable in tests.
// #[automock] TODO
#[async_trait]
pub trait LogsBackend: Send + Sync {
    async fn create_log_stream(
        &self,
        log_group: String,
        log_stream: String,
    ) -> Result<CreateLogStreamOutput, SdkError<CreateLogStreamError>>;

    async fn describe_log_stream(
        &self,
        log_group: String,
        log_stream: String,
    ) -> Result<DescribeLogStreamsOutput, SdkError<DescribeLogStreamsError>>;

    async fn put_log_events(
        &self,
        log_group: String,
        log_stream: String,
        events: Vec<InputLogEvent>,
    ) -> Result<PutLogEventsOutput, SdkError<PutLogEventsError>>;
}

#[async_trait]
impl LogsBackend for aws_sdk_cloudwatchlogs::Client {
    async fn create_log_stream(
        &self,
        log_group: String,
        log_stream: String,
    ) -> Result<CreateLogStreamOutput, SdkError<CreateLogStreamError>> {
        self.create_log_stream()
            .log_group_name(log_group)
            .log_stream_name(log_stream)
            .send()
            .await
    }

    async fn describe_log_stream(
        &self,
        log_group: String,
        log_stream: String,
    ) -> Result<DescribeLogStreamsOutput, SdkError<DescribeLogStreamsError>> {
        self.describe_log_streams()
            .log_group_name(log_group)
            .log_stream_name_prefix(log_stream)
            .limit(1)
            .send()
            .await
    }

    async fn put_log_events(
        &self,
        log_group: String,
        log_stream: String,
        events: Vec<InputLogEvent>,
    ) -> Result<PutLogEventsOutput, SdkError<PutLogEventsError>> {
        self.put_log_events()
            .log_group_name(log_group)
            .log_stream_name(log_stream)
            .set_log_events(Some(events))
            .send()
            .await
    }
}

// Register dependency, default to real AWS backend.
// --------------------------------------------------

register_ctx_singleton!(dyn LogsCtxView, dyn LogsBackend, |ctx: Arc<
    dyn LogsCtxView,
>| async move {
    let region = Region::new(ctx.log_region().clone());
    let shared_config = aws_config::defaults(BehaviorVersion::v2025_08_07())
        .region(region)
        .load()
        .await;
    Ok(aws_sdk_cloudwatchlogs::Client::new(&shared_config))
});
