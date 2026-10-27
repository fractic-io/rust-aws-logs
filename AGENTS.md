# fractic-aws-logs

Wrapper over AWS CloudWatch Logs and metrics for the Fractic backends:
`LogsUtil` (structured log / metric writes), `LogsCtxView`, typed errors.
Public, provided as-is (`fractic-io/rust-aws-logs`, identity `fractic`).

## Layout

`src/lib.rs` → `context.rs`, `errors.rs`, `util.rs` (+ `util/backend.rs`
for the SDK calls behind the util).

## Read first

- `~/.agents/AGENTS.md` (workspace orientation; loaded automatically by Claude
  Code, read it if you are Codex or Grok) and `~/.agents/guides/rust.md`
  (style and the library layouts in §2).

## Working here

- No flake here: build and test through a product devshell, e.g.
  `nix develop /efs/fractic/storyengine/util --command cargo test`, and run
  `cargo clippy --all-targets -- -D warnings`.
- Consumers depend on the git URL and `[patch]` it to this checkout, so a
  local change is visible to them immediately; the pushed revision is picked
  up with `cargo update -p <crate>`. Flag breaking changes in the PR title
  (`Breaking: …`).
- Errors are `define_internal_error!` / `define_user_error!` types in
  `errors.rs`; context requirements are a `define_ctx_view!` in `context.rs`
  (env vars the consumer must provide); the public surface is `util.rs`
  (`<Name>Util`) plus `pub use`s in `lib.rs`. Keep that shape; add a new
  capability as a method on the util or a new `ext/` module, not a parallel
  façade.
- Commit / PR titles: `[util] …`, `[ext] …`, imperative, trailing period; the
  squash merge appends the PR number.
