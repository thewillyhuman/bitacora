# Prometheus

| Path | What it is |
| --- | --- |
| `bitacora-alerts.yaml` | The default alert rules of a bitacora cluster |
| `pint.hcl` | What every alert must carry: a severity, a summary, a description |
| `alerts_test.sh` | The test of the rules |

`docs/operations/alerts.md` lists the alerts, says what to do first for each, and says what they cannot
see. `docs/operations/metrics.md` is the reference of the metrics they are written against.

## Using the rules

Load the file as it is:

```yaml
# prometheus.yml
rule_files:
  - /etc/prometheus/rules/bitacora-alerts.yaml

scrape_configs:
  - job_name: bitacora
    static_configs:
      - targets:
          - replica-a.example.org:9100
          - replica-b.example.org:9100
          - replica-c.example.org:9100
          - controller-a.example.org:9101
          - controller-b.example.org:9101
```

The daemons expose their metrics on the address their configuration names as
`metrics_listen_address`: port 9100 for a replica and 9101 for a controller in the example
configurations.

The rules rely on two labels only, `job` and `instance`, which Prometheus adds to every target. Use one
job per cluster, or one for its replicas and one for its controllers; alerts about a cluster as a whole
are grouped by `job`, so two clusters under one job would be judged as one.

On Kubernetes, the Helm chart can create a `ServiceMonitor` for the Prometheus operator
(`src/helm/bitacora/README.md`). The rules are not packaged as a `PrometheusRule`: wrap the `groups` of
this file in one if you use the operator.

The thresholds quote the defaults of the daemons. If you run with other values for the settings named at
the top of the file, change the alerts that quote them.

## How the rules are tested

Without a Prometheus server:

- `//src/prometheus:alerts_test` lints the file with the pinned `pint`, which parses every expression as
  PromQL and every annotation as a template, and requires a `severity`, a `summary` and a `description`
  on every alert. It then checks what a linter cannot know: every metric and every label value the rules
  use is in the metrics reference; `docs/operations/alerts.md` lists exactly the alerts that exist, with
  their severities; and the defaults of the daemons that the rules quote are still the defaults.
- `//src/prometheus:alerts_rejection_test` runs the same checker on a copy of the rules with one mistake
  of each of those kinds and requires it to fail and to name every one.

What is not tested: whether an alert fires when it should. That needs a time series to evaluate the
expressions against (`promtool test rules`), which this repository does not do; the expressions are
parsed and their metrics exist, and that is all the tests say.
