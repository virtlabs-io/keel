# KEEL Monitoring

This folder contains pre-built observability assets for KEEL.

## Files

| File | Purpose |
|------|---------|
| `keel-grafana.json` | Grafana dashboard — import via Grafana UI or provisioning |
| `keel-rules.yml` | Prometheus alerting + recording rules |

---

## Grafana Dashboard

### Import via UI

1. Open Grafana → **Dashboards** → **Import**
2. Upload `keel-grafana.json` or paste its contents
3. Select the Prometheus datasource pointed at KEEL's `/metrics` endpoint
4. Click **Import**

### Import via provisioning

Copy the dashboard JSON to your Grafana provisioning directory:

```bash
cp monitoring/keel-grafana.json \
   /etc/grafana/provisioning/dashboards/keel-grafana.json
```

Add a provisioning config if you don't have one:

```yaml
# /etc/grafana/provisioning/dashboards/keel.yaml
apiVersion: 1
providers:
  - name: keel
    folder: KEEL
    type: file
    options:
      path: /etc/grafana/provisioning/dashboards
```

### Kubernetes / Helm provisioning

If you use the Grafana Helm chart with sidecar dashboards:

```yaml
# values.yaml for grafana helm chart
sidecar:
  dashboards:
    enabled: true
    label: grafana_dashboard
```

Then create a ConfigMap:

```bash
kubectl create configmap keel-grafana-dashboard \
  --from-file=monitoring/keel-grafana.json \
  -n monitoring \
  --dry-run=client -o yaml \
  | kubectl label --local -f - grafana_dashboard=1 -o yaml \
  | kubectl apply -f -
```

---

## Prometheus Rules

### Direct Prometheus

Add the rules file to your Prometheus config:

```yaml
# prometheus.yml
rule_files:
  - /etc/prometheus/rules/keel-rules.yml
```

Copy the file:

```bash
cp monitoring/keel-rules.yml /etc/prometheus/rules/
# Reload Prometheus
kill -HUP $(pidof prometheus)
# or: curl -X POST http://localhost:9090/-/reload
```

### Prometheus Operator (PrometheusRule CRD)

```bash
kubectl apply -f - <<EOF
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: keel-rules
  namespace: monitoring
  labels:
    prometheus: kube-prometheus
    role: alert-rules
spec:
$(sed 's/^/  /' monitoring/keel-rules.yml)
EOF
```

---

## KEEL Prometheus configuration

Enable the metrics endpoint in `keel.ini`:

```ini
[prometheus]
enabled = true
listen_addr = 0.0.0.0
port = 9101
path = /metrics
```

Add a Prometheus scrape job:

```yaml
# prometheus.yml
scrape_configs:
  - job_name: keel
    static_configs:
      - targets: ["keel-host:9101"]
    # Or use Kubernetes service discovery:
    # kubernetes_sd_configs: ...
    # relabel_configs: ...
```

If you use the Helm chart, the `ServiceMonitor` is already included when
`serviceMonitor.enabled: true` is set in `values.yaml`.

---

## Alerts summary

| Alert | Severity | Fires when |
|-------|----------|------------|
| `KeelPoolNearSaturation` | warning | utilization > 85% for 2m |
| `KeelPoolSaturated` | critical | utilization > 98% for 1m |
| `KeelWaitingSessionsHigh` | warning | > 50 sessions in wait queue |
| `KeelQueryLatencyP99High` | warning | P99 query latency > 500ms for 5m |
| `KeelBackendLatencyP95High` | warning | P95 backend latency > 200ms for 5m |
| `KeelConnectLatencyHigh` | warning | P95 connect latency > 100ms for 3m |
| `KeelDown` | critical | no metrics for 1m |
| `KeelHighFDUsage` | warning | FD usage > 80% of limit for 5m |
| `KeelTLSDowngradeAttempts` | warning | any TLS downgrade rejected |
| `KeelTLSCertReloadFailure` | critical | cert reload fails |
| `KeelClusterPeerDown` | warning | peer unreachable for 2m |
| `KeelClusterNoLeader` | critical | no elected leader for 30s |
| `KeelRoutingFailoverHigh` | warning | > 10 failover routes/s for 2m |
