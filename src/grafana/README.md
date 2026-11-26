# Grafana

| Path | What it is |
| --- | --- |
| `bitacora-dashboard.json` | One dashboard for one bitacora cluster |
| `dashboard_test.sh` | The test of the dashboard |

## The dashboard

It reads a cluster by component, top to bottom, in the order you would ask the questions:

| Row | Answers |
| --- | --- |
| Cluster | Does a controller lead, are segments short of copies, how many replicas report, is anything damaged, draining or cut off from etcd |
| Replicas: the write path | How long flushes take, how many appends are served and how long they take inside a replica, what is refused |
| Replicas: disks and copies | How full the disks are, how much is written, how many copies each replica holds, whether replicas register and scrub |
| Metadata store | How etcd answers bitacora's processes: rate, latency, failures, availability |
| Object store | Uploads, reads, failures and bytes moved |
| Controller | Who leads, how long a pass takes, what the passes do: placement, repair, tiering, deletion, snapshots |

Every panel has a description that says how to read it. The metrics are those of
`docs/operations/metrics.md`; the dashboard shows every one of them.

It does not show append latency as an application sees it, failover time, or materializer lag: those are
only known inside the application (see "What is not measured" in the metrics reference).

## Using it

Import `bitacora-dashboard.json` in Grafana (Dashboards, New, Import), or provision it from a directory.
It asks for two things at the top:

- **Data source**: the Prometheus that scrapes the cluster.
- **Job**: the scrape jobs of the cluster to look at. It lists the jobs that expose bitacora's metrics;
  choose the ones of one cluster.

The dashboard has the fixed identifier `bitacora`, so importing a newer version replaces the old one and
links to it keep working.

## How it is tested

Without Grafana and without Prometheus:

- `//src/grafana:dashboard_test` reads the file with the pinned `jq` and checks that every panel has a
  title, a description, a query and the data source chosen at the top, that panel identifiers are unique
  and panels fit the grid; that every query selects the chosen job for every metric it reads; that every
  metric and label value is in the metrics reference, and that every metric of the reference is shown by
  some panel. It hands every query to the pinned `pint`, with Grafana's variables replaced, to check that
  it is valid PromQL.
- `//src/grafana:dashboard_rejection_test` runs the same checker on a copy of the dashboard with one
  mistake of each of those kinds and requires it to fail and to name every one.

What is not tested: how the dashboard looks, and whether it loads in a particular version of Grafana. It
was written against schema version 39 and has not been opened in a running Grafana.
