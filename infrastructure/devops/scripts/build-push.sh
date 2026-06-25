#!/usr/bin/env bash
#
# build-push.sh — host-neutral Docker build-and-push for the Dagster
# code-server and related images.
#
# This script contains ZERO CI-host-specific logic. Azure Pipelines, GitHub
# Actions and GitLab CI can all invoke it the same way — each only needs a
# thin wrapper that (a) authenticates to the registry and (b) maps its own
# native build-metadata variables onto the env vars below. That keeps a future
# migration between git hosts down to swapping the ~10-line wrapper, not the
# build logic. See task #28544.
#
# Registry authentication is the CALLER's responsibility — it is the one piece
# that is inherently host-specific. Either authenticate before calling (e.g.
# `az acr login`, `docker/login-action`, `docker login`), OR set
# REGISTRY_USERNAME / REGISTRY_PASSWORD and this script will `docker login`.
#
# ─── Required env ───────────────────────────────────────────────────────────
#   REGISTRY      Container registry / login server  (e.g. myregistry.example.io)
#   IMAGE_NAME    Image repository path              (e.g. org/dagster-code-server)
#   DOCKERFILE    Path to the Dockerfile
#   TAGS          Space-separated list of tags to apply (at least one)
#
# ─── Optional env ───────────────────────────────────────────────────────────
#   BUILD_CONTEXT     Build context dir                 (default: .)
#   PLATFORM          Target platform; flag omitted if empty   (default: <unset>)
#   USE_CACHE         true|false — registry layer cache (default: true)
#   CACHE_REF         Cache image ref     (default: ${REGISTRY}/${IMAGE_NAME}-cache:buildcache)
#   BUILDER_NAME      buildx builder name               (default: hp-builder)
#   BUILD_TIMESTAMP   Cache-bust build-arg value        (default: UTC yyyymmddHHMMSS)
#   EXTRA_BUILD_ARGS  Extra build args, space-separated KEY=VALUE pairs
#   EXTRA_LABELS      Extra image labels, space-separated key=value pairs
#   GIT_SHA           Commit SHA  -> org.opencontainers.image.revision label
#   REPO_URI          Repo URL    -> org.opencontainers.image.source label
#   REGISTRY_USERNAME / REGISTRY_PASSWORD   If both set, script runs docker login
#   DRY_RUN           1|true — print the build command instead of executing it
#
# Exits non-zero on missing required env or build failure.
#
set -euo pipefail

log() { printf '%s\n' "$*"; }
die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

# ─── Validate required inputs ────────────────────────────────────────────────
: "${REGISTRY:?REGISTRY is required (registry / login server)}"
: "${IMAGE_NAME:?IMAGE_NAME is required (image repository path)}"
: "${DOCKERFILE:?DOCKERFILE is required (path to Dockerfile)}"
: "${TAGS:?TAGS is required (space-separated list, at least one)}"

# ─── Defaults ────────────────────────────────────────────────────────────────
BUILD_CONTEXT="${BUILD_CONTEXT:-.}"
PLATFORM="${PLATFORM:-}"
USE_CACHE="${USE_CACHE:-true}"
CACHE_REF="${CACHE_REF:-${REGISTRY}/${IMAGE_NAME}-cache:buildcache}"
BUILDER_NAME="${BUILDER_NAME:-hp-builder}"
BUILD_TIMESTAMP="${BUILD_TIMESTAMP:-$(date -u +%Y%m%d%H%M%S)}"
EXTRA_BUILD_ARGS="${EXTRA_BUILD_ARGS:-}"
EXTRA_LABELS="${EXTRA_LABELS:-}"
GIT_SHA="${GIT_SHA:-}"
REPO_URI="${REPO_URI:-}"
DRY_RUN="${DRY_RUN:-}"

[ -f "$DOCKERFILE" ] || die "Dockerfile not found: $DOCKERFILE"

is_dry_run() { [ "$DRY_RUN" = "1" ] || [ "$DRY_RUN" = "true" ]; }

# Side effects (login, builder setup, build) are skipped on DRY_RUN so the
# script can be validated without Docker or registry access.
if ! is_dry_run; then
  # ─── Optional registry login (only when creds are supplied) ────────────────
  if [ -n "${REGISTRY_USERNAME:-}" ] && [ -n "${REGISTRY_PASSWORD:-}" ]; then
    log "Logging in to ${REGISTRY} as ${REGISTRY_USERNAME}"
    printf '%s' "$REGISTRY_PASSWORD" | docker login "$REGISTRY" \
      --username "$REGISTRY_USERNAME" --password-stdin
  else
    log "No REGISTRY_USERNAME/REGISTRY_PASSWORD set — assuming caller is already authenticated to ${REGISTRY}"
  fi

  # ─── buildx builder (docker-container driver enables registry cache export) ─
  docker buildx create --name "$BUILDER_NAME" --driver docker-container --use 2>/dev/null \
    || docker buildx use "$BUILDER_NAME"
  docker buildx inspect --bootstrap >/dev/null
fi

# ─── Assemble the build command (arrays keep quoting safe) ───────────────────
cmd=(docker buildx build --file "$DOCKERFILE")

for tag in $TAGS; do
  cmd+=(--tag "${REGISTRY}/${IMAGE_NAME}:${tag}")
done

[ -n "$PLATFORM" ] && cmd+=(--platform "$PLATFORM")

cmd+=(--build-arg "BUILD_TIMESTAMP=${BUILD_TIMESTAMP}")
for kv in $EXTRA_BUILD_ARGS; do
  cmd+=(--build-arg "$kv")
done

# OCI-standard provenance labels
cmd+=(--label "org.opencontainers.image.created=$(date -u +%Y-%m-%dT%H:%M:%SZ)")
[ -n "$GIT_SHA" ]  && cmd+=(--label "org.opencontainers.image.revision=${GIT_SHA}")
[ -n "$REPO_URI" ] && cmd+=(--label "org.opencontainers.image.source=${REPO_URI}")
for kv in $EXTRA_LABELS; do
  cmd+=(--label "$kv")
done

# Registry layer cache (falls back to a clean build when disabled)
if [ "$USE_CACHE" = "true" ] || [ "$USE_CACHE" = "True" ]; then
  cmd+=(--cache-from "type=registry,ref=${CACHE_REF}")
  cmd+=(--cache-to "type=registry,ref=${CACHE_REF},mode=max")
else
  cmd+=(--no-cache)
fi

cmd+=(--push --provenance=false "$BUILD_CONTEXT")

# ─── Report + execute ────────────────────────────────────────────────────────
log "=============================================="
log "Building ${REGISTRY}/${IMAGE_NAME}"
log "  Dockerfile : ${DOCKERFILE}"
log "  Tags       : ${TAGS}"
log "  Platform   : ${PLATFORM:-<default>}"
log "  Cache      : ${USE_CACHE} (${CACHE_REF})"
log "=============================================="

if is_dry_run; then
  log "DRY_RUN — command that would run:"
  printf '%q ' "${cmd[@]}"
  printf '\n'
  exit 0
fi

export DOCKER_BUILDKIT=1
"${cmd[@]}"

log "Build and push complete."
for tag in $TAGS; do
  log "  -> ${REGISTRY}/${IMAGE_NAME}:${tag}"
done
