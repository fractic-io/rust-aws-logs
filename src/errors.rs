use fractic_server_error::{define_client_error, define_internal_error, define_user_error};

define_client_error!(LogGroupNotFound, "Log group does not exist: {log_group}.", { log_group: &str });
define_internal_error!(CloudWatchCalloutError, "CloudWatch callout error: {details}.", { details: &str });
define_client_error!(CloudWatchInvalidOperation, "Invalid CloudWatch operation: {details}.", { details: &str });
