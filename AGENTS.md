# fractic-aws-logs

Wrapper over AWS CloudWatch Logs and metrics: `LogsUtil` (structured log /
metric writes), `LogsCtxView`, typed errors. `src/lib.rs` → `context.rs`,
`errors.rs`, `util.rs` (+ `util/backend.rs` for the SDK calls).
