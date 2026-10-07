#!/usr/bin/env bash
# Cluster + infrastructure orchestration for the devtools/ toolkit. Source, don't
# execute. Depends on common.sh already being sourced.

STATE_FILE="${GEN_DIR}/state.env"

# --- State (which profiles / topology the running env was brought up with) ---
save_state() {
  mkgen
  cat > "${STATE_FILE}" <<EOF
DEV_PROFILES="${DEV_PROFILES:-}"
DEV_FULL="${DEV_FULL:-false}"
DEV_DOMAIN="${CONSOLE_CLUSTER_DOMAIN}"
EOF
}

load_state() {
  # shellcheck disable=SC1090
  [ -f "${STATE_FILE}" ] && source "${STATE_FILE}" || true
  DEV_PROFILES="${DEV_PROFILES:-}"
  DEV_FULL="${DEV_FULL:-false}"
  # Restore the domain the environment was brought up with, so commands run in a
  # fresh shell (config/backend/operator/urls/status) generate hosts that match
  # the live cluster. An explicit CONSOLE_CLUSTER_DOMAIN in this shell still wins.
  if [ -z "${CONSOLE_CLUSTER_DOMAIN_OVERRIDE:-}" ] && [ -n "${DEV_DOMAIN:-}" ]; then
    export CONSOLE_CLUSTER_DOMAIN="${DEV_DOMAIN}"
  fi
}

has_profile() {
  case " ${DEV_PROFILES} " in *" $1 "*) return 0 ;; *) return 1 ;; esac
}

# --- Cluster ----------------------------------------------------------------
ensure_cluster() {
  info "Ensuring kind cluster '${CLUSTER_NAME}' (engine=${CONTAINER_ENGINE})"
  # Delegate to the repo's macOS-verified bootstrap: ports 80/443 -> host,
  # ingress-nginx via hostPort, SSL passthrough, end-to-end smoke test.
  CONTAINER_ENGINE="${CONTAINER_ENGINE}" \
  CLUSTER_NAME="${CLUSTER_NAME}" \
  CONSOLE_CLUSTER_DOMAIN="${CONSOLE_CLUSTER_DOMAIN}" \
    "${CLUSTER_SETUP_DIR}/kind/create-cluster.sh"
  kubectl config use-context "${KUBECONTEXT}" >/dev/null
}

# --- Strimzi ----------------------------------------------------------------
install_strimzi() {
  if helm status strimzi-cluster-operator -n "${STRIMZI_NAMESPACE}" >/dev/null 2>&1; then
    step "Strimzi already installed in '${STRIMZI_NAMESPACE}'"
  else
    info "Installing Strimzi ${STRIMZI_VERSION} (watching all namespaces)"
    kubectl create namespace "${STRIMZI_NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
    helm install strimzi-cluster-operator \
      oci://quay.io/strimzi-helm/strimzi-kafka-operator \
      --version "${STRIMZI_VERSION}" \
      --namespace "${STRIMZI_NAMESPACE}" \
      --set watchAnyNamespace=true \
      --wait
  fi
  kubectl -n "${STRIMZI_NAMESPACE}" wait --for=condition=available \
    deployment/strimzi-cluster-operator --timeout=180s
}

# --- Kafka ------------------------------------------------------------------
deploy_kafka() {
  kubectl create namespace "${KAFKA_NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  if [ "${DEV_FULL}" = "true" ]; then
    info "Deploying full Kafka topology (3 brokers + 3 controllers, Cruise Control) from examples/kafka"
    export CLUSTER_DOMAIN="${CONSOLE_CLUSTER_DOMAIN}"
    export NAMESPACE="${KAFKA_NAMESPACE}"
    export LISTENER_TYPE="ingress"
    cat "${REPO_ROOT}"/examples/kafka/*.yaml \
      | envsubst "${TEMPLATE_VARS}" \
      | kubectl apply -n "${KAFKA_NAMESPACE}" -f -
    # Only add the internal 'plain' listener if it isn't already there. Checking
    # first (rather than swallowing patch errors) means a genuine patch failure
    # surfaces instead of being reported as 'already present'.
    if kubectl -n "${KAFKA_NAMESPACE}" get kafka "${KAFKA_NAME}" \
         -o jsonpath='{.spec.kafka.listeners[?(@.name=="plain")].name}' 2>/dev/null | grep -q .; then
      step "(plain listener already present)"
    else
      step "Adding internal 'plain' listener for in-cluster clients (Connect, operator-mode console)"
      kubectl -n "${KAFKA_NAMESPACE}" patch kafka "${KAFKA_NAME}" --type=json \
        -p='[{"op":"add","path":"/spec/kafka/listeners/-","value":{"name":"plain","port":9092,"type":"internal","tls":false}}]'
    fi
  else
    info "Deploying lean Kafka topology (single dual-role KRaft node)"
    # Reuse the documented JMX-exporter metrics rules rather than duplicating them.
    kubectl apply -n "${KAFKA_NAMESPACE}" \
      -f "${REPO_ROOT}/examples/kafka/010-ConfigMap-console-kafka-metrics.yaml" >/dev/null
    apply_templated "${MANIFESTS_DIR}/kafka-lean/kafka.yaml" "${KAFKA_NAMESPACE}"
  fi
  info "Waiting for Kafka '${KAFKA_NAME}' to become Ready (first run pulls images, can take a few minutes)"
  kubectl -n "${KAFKA_NAMESPACE}" wait --for=condition=Ready "kafka/${KAFKA_NAME}" --timeout=600s
}

# --- Profiles ---------------------------------------------------------------
install_prometheus_operator() {
  if kubectl get deployment prometheus-operator -n default >/dev/null 2>&1; then
    step "Prometheus operator already installed"
  else
    info "Installing Prometheus operator ${PROMETHEUS_OPERATOR_VERSION}"
    kubectl apply --server-side --force-conflicts -f \
      "https://github.com/prometheus-operator/prometheus-operator/releases/download/${PROMETHEUS_OPERATOR_VERSION}/bundle.yaml"
  fi
  kubectl -n default rollout status deployment/prometheus-operator --timeout=300s
}

deploy_profile_metrics() {
  info "Profile: metrics (Prometheus)"
  install_prometheus_operator
  apply_templated "${MANIFESTS_DIR}/profiles/metrics/prometheus.yaml" "metrics"
  apply_templated "${MANIFESTS_DIR}/profiles/metrics/podmonitor.yaml" "${KAFKA_NAMESPACE}"
}

deploy_profile_registry() {
  info "Profile: registry (Apicurio)"
  apply_templated "${MANIFESTS_DIR}/profiles/registry/registry.yaml" "registry"
}

deploy_profile_keycloak() {
  info "Profile: keycloak (OIDC)"
  local tmp; tmp="$(mktemp)"
  export CLUSTER_DOMAIN="${CONSOLE_CLUSTER_DOMAIN}"
  envsubst "${TEMPLATE_VARS}" < "${MANIFESTS_DIR}/profiles/keycloak/streamshub-realm.json" > "${tmp}"
  kubectl create namespace keycloak --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  kubectl -n keycloak create configmap keycloak-realm \
    --from-file=streamshub-realm.json="${tmp}" \
    --dry-run=client -o yaml | kubectl apply -f -
  rm -f "${tmp}"
  apply_templated "${MANIFESTS_DIR}/profiles/keycloak/keycloak.yaml" "keycloak"
}

deploy_profile_connect() {
  info "Profile: connect (Kafka Connect)"
  apply_templated "${MANIFESTS_DIR}/profiles/connect/connect.yaml" "${KAFKA_NAMESPACE}"
}

deploy_profiles() {
  has_profile metrics  && deploy_profile_metrics
  has_profile registry && deploy_profile_registry
  has_profile keycloak && deploy_profile_keycloak
  has_profile connect  && deploy_profile_connect
  return 0
}

# Poll (up to $3 seconds, default 120) for a named resource to exist in ns $2.
wait_for_resource() {
  local res="$1" ns="$2" timeout="${3:-120}" waited=0
  while [ "${waited}" -lt "${timeout}" ]; do
    kubectl -n "${ns}" get "${res}" >/dev/null 2>&1 && return 0
    sleep 2; waited=$((waited + 2))
  done
  return 1
}

wait_profiles_ready() {
  if has_profile metrics; then
    # The Prometheus operator creates this StatefulSet asynchronously after the
    # Prometheus CR is applied, so wait for it to appear before checking rollout
    # (a bare `rollout status` would immediately fail with 'not found').
    if wait_for_resource statefulset/prometheus-console-prometheus metrics 120; then
      kubectl -n metrics rollout status statefulset/prometheus-console-prometheus --timeout=300s || true
    else
      warn "Prometheus StatefulSet did not appear within 120s; metrics may not be ready yet"
    fi
  fi
  has_profile registry && kubectl -n registry rollout status deployment/apicurio-registry --timeout=300s || true
  has_profile keycloak && kubectl -n keycloak rollout status deployment/keycloak --timeout=300s || true
  has_profile connect  && kubectl -n "${KAFKA_NAMESPACE}" wait --for=condition=Ready kafkaconnect/console-connect --timeout=300s || true
  return 0
}

# --- Teardown ---------------------------------------------------------------
teardown_full() {
  info "Deleting kind cluster '${CLUSTER_NAME}' (full teardown)"
  kind delete cluster --name "${CLUSTER_NAME}"
  rm -f "${STATE_FILE}" "${GEN_DIR}/console-config.yaml" "${GEN_DIR}/console-cr.yaml" "${GEN_DIR}/ca.crt"
}

teardown_keep_cluster() {
  ensure_context
  info "Removing deployed resources (keeping cluster + ingress-nginx)"
  # Delete Strimzi/Console CRs before their namespaces to avoid the
  # finalizer/namespace-Terminating race (same reason as cluster-setup).
  kubectl delete console,kafkaconnect,kafkatopic,kafkauser,kafka,kafkanodepool \
    -n "${KAFKA_NAMESPACE}" --all --ignore-not-found --timeout=120s || true
  for ns in "${KAFKA_NAMESPACE}" metrics registry keycloak; do
    kubectl delete namespace "${ns}" --ignore-not-found --timeout=120s || true
  done
  # The metrics profile creates cluster-scoped RBAC that deleting the namespace
  # leaves behind, so remove it explicitly.
  kubectl delete clusterrole,clusterrolebinding console-prometheus-server \
    --ignore-not-found || true
  helm uninstall strimzi-cluster-operator -n "${STRIMZI_NAMESPACE}" 2>/dev/null || true
  kubectl delete namespace "${STRIMZI_NAMESPACE}" --ignore-not-found || true
  rm -f "${STATE_FILE}" "${GEN_DIR}/console-config.yaml" "${GEN_DIR}/console-cr.yaml" "${GEN_DIR}/ca.crt"
}
