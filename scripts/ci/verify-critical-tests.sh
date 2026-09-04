#!/usr/bin/env bash
set -euo pipefail

result_file="$(mktemp)"
trap 'rm -f "${result_file}"' EXIT

critical_tests=(
  TestPublicBindingsPreserveModuleAndSaaSBusinessSemantics
  TestIntegrationDoesNotOwnConcreteProvidersOrRuntimeOutbox
  TestFactoryAssemblesDeploymentNeutralModuleBinding
  TestSecretRotationRollsBackMaterialAndMetadataTogether
  TestOperationsStoreOwnsCallWebhookMappingAndRuntimeReceipt
  TestIntegrationCapabilityTracksRoutesConnectorsAndValidation
  TestAuthorizeActionUsesTheExactPermissionAndAllowsPersonalOwnerScope
)

test_pattern="^($(IFS='|'; echo "${critical_tests[*]}"))$"

go test -race -count=1 -json -run "${test_pattern}" \
  ./internal/architecture \
  ./internal/assembly/module \
  ./internal/assembly/saas \
  ./internal/infrastructure/persistence/database/integration \
  ./internal/transport/http/module | tee "${result_file}"

for test_name in "${critical_tests[@]}"; do
  if ! grep -Eq "\"Action\":\"pass\".*\"Test\":\"${test_name}\"" "${result_file}"; then
    echo "critical test did not execute and pass: ${test_name}" >&2
    exit 1
  fi
done
