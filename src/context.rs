use fractic_context::define_ctx_view;

define_ctx_view!(
    name: LogsCtxView,
    env {
        LOG_REGION: String,
    },
    secrets {},
    deps_overlay {
        dyn crate::util::backend::LogsBackend,
    },
    req_impl {}
);

#[cfg(test)]
pub(crate) mod test_ctx {
    #![allow(dead_code)] // Remove once test coverage is added.

    use fractic_context::define_ctx;

    define_ctx!(
        name: TestLogsCtx,
        env {
            LOG_REGION: String,
        },
        secrets_fetch_region: DUMMY,
        secrets_fetch_id: DUMMY,
        secrets {},
        deps {},
        views {
            crate::LogsCtxView,
        }
    );
}
