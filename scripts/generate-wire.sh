#!/usr/bin/env bash
# Regenerates schema/generated/facetql_wire.rs from the canonical schema in
# the sibling fct repo (schema/facetql_wire.fct) — the facetql-side half of
# SCHEMA_IDL_SCOPE.md §4 item 4's "one command per repo". The schema lives in
# fct because FCT is the schema language (Tier C: FCT-as-IDL); this script is
# the thin cross-repo half that makes facetql's copy one command too.
#
# Deliberately NOT under src/ and NOT wired into the crate (no `mod wire;` in
# main.rs/lib.rs) — two reasons, not one: it generates the artifact and
# proves the pipeline without committing to the separate, higher-stakes
# cutover of routes.rs's hand-written types (see fct/schema/facetql_wire.fct's
# header and this session's report); and integration/stack_test.go's
# stale-binary guard (fct repo) conservatively treats ANY newer .rs file
# under facetql/src/ as "rebuild needed" — an orphaned, never-compiled file
# there would trip that guard on every run for no real reason. schema/ is
# where fct's own generated output already lives, so this mirrors that.
#
# Requires the fct repo checked out as a sibling directory (the convention
# this whole project already uses — see AGENT_LOG.md's cross-repo notes).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
FCT_DIR="../fct"
if [ ! -d "$FCT_DIR" ]; then
  echo "expected the fct repo at $FCT_DIR (sibling checkout) — not found" >&2
  exit 1
fi
mkdir -p schema/generated
( cd "$FCT_DIR" && go run ./cmd/schemagen schema/facetql_wire.fct /tmp/facetql_wire_gen wire )
cp /tmp/facetql_wire_gen.rs schema/generated/facetql_wire.rs
rm -f /tmp/facetql_wire_gen.rs /tmp/facetql_wire_gen.go /tmp/facetql_wire_gen.ts
echo "wrote schema/generated/facetql_wire.rs"
