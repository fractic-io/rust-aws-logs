# fractic-aws-logs

Wrapper over AWS CloudWatch Logs and metrics: `LogsUtil` (structured log /
metric writes), `LogsCtxView`, typed errors. `src/lib.rs` → `context.rs`,
`errors.rs`, `util.rs` (+ `util/backend.rs` for the SDK calls).

- Keep the shape: errors in `errors.rs`, context requirements in
  `context.rs`, the public surface `util.rs` (`LogsUtil`) plus `pub use`s in
  `lib.rs`; `backend.rs` is the only place that calls the SDK. A new
  capability is a method on the util or an `ext/` module.
- Commit / PR titles: `[util] …`, `[ext] …`.
