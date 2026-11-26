# Settings of pint, the linter of the alert rules in this directory.
# alerts_test.sh runs it offline: no check asks a Prometheus server.

# What every alert must carry, so that it can be routed and acted on.
rule {
  match {
    kind = "alerting"
  }

  # Who is woken: `critical` pages a person, `warning` opens a ticket.
  label "severity" {
    severity = "bug"
    value    = "(warning|critical)"
    required = true
  }

  # One line for the notification.
  annotation "summary" {
    severity = "bug"
    required = true
  }

  # What it means and what to do.
  annotation "description" {
    severity = "bug"
    required = true
  }
}
