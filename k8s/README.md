# FlowRulZ on Kubernetes

Run the FlowRulZ distributed rule broker on Kubernetes.

## Quick start (kind)

```bash
# Build the image and create a kind cluster
make kind-deploy

# Verify
kubectl get pods -n flowrulz
kubectl logs -n flowrulz -l app=flowrulz
```

## Helm

```bash
# Install
make helm-install

# Override values
helm upgrade --install flowrulz k8s/helm \
  --namespace flowrulz --create-namespace \
  --set flowrulz.replicas=5 \
  --set flowrulz.apiKey=my-secret-key

# Uninstall
make helm-uninstall
```

## Kustomize

```bash
# Deploy
make k8s-deploy

# Delete
make k8s-delete
```

## Access

| Component     | Host                   | Port |
|---------------|------------------------|------|
| Admin API     | flowrulz.local/api     | 80   |
| Health        | flowrulz.local/health  | 80   |
| Sim dashboard | sim.flowrulz.local     | 80   |

Add to `/etc/hosts` for local kind:

```
127.0.0.1 flowrulz.local sim.flowrulz.local
```

## Architecture

- **Production nodes**: 3-node StatefulSet with headless Service, PVC-backed persistence, Cluster Bus discovery via `FLOWRULZ_SEEDS`
- **Simulator**: Single-replica Deployment with interactive dashboard, for testing rules and services
- **Ingress**: nginx-based routing for admin API and simulator dashboard

## Env vars

See [docs/](/docs/) for the full env var reference.

## Images

- `flowrulz:latest` — production binary (`docker build --target flowrulz .`)
- `flowrulz-sim:latest` — simulator binary (`docker build --target sim .`)

For production, push to your registry and override `image.repository` in Helm values.
