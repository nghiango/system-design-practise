#!/usr/bin/env bash
# Build (and with PUSH_IMAGES=true, push) the ARM64 image of every service of one topic:
#   $IMAGE_PREFIX/<topic>-<service>:$REVISION from topics/<topic>/services/<service>/Dockerfile
set -euo pipefail
topic=${1:?usage: build_images.sh <topic>}
: "${IMAGE_PREFIX:?}" "${REVISION:?}"
[[ "$topic" =~ ^[a-z][a-z0-9-]{1,30}$ ]] || { echo "invalid topic $topic" >&2; exit 2; }
[[ "$REVISION" =~ ^[0-9a-f]{40}$ ]] || { echo "REVISION must be a full commit SHA" >&2; exit 2; }
cd "$(dirname "$0")/../.."
[[ -f "topics/$topic/topic.yaml" ]] || { echo "unknown topic $topic" >&2; exit 2; }

shopt -s nullglob
dockerfiles=(topics/"$topic"/services/*/Dockerfile)
if (( ${#dockerfiles[@]} == 0 )); then
  echo "no services for $topic"
  exit 0
fi

if ! docker buildx inspect hyperscale-local >/dev/null 2>&1; then
  docker buildx create --name hyperscale-local --node hyperscale-local0 \
    --driver docker-container --driver-opt network=kind \
    --buildkitd-config ci/buildkitd.toml
fi
docker buildx inspect hyperscale-local --bootstrap >/dev/null
output_flag=(--load)
[[ "${PUSH_IMAGES:-false}" == "true" ]] && output_flag=(--push)

for dockerfile in "${dockerfiles[@]}"; do
  dir=$(dirname "$dockerfile")
  service=$(basename "$dir")
  echo "building $IMAGE_PREFIX/$topic-$service:$REVISION"
  docker buildx build --builder hyperscale-local --platform linux/arm64 "${output_flag[@]}" \
    --tag "$IMAGE_PREFIX/$topic-$service:$REVISION" "$dir"
done
