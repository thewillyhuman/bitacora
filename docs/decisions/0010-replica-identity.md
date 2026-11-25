# 0010. A replica is reached by identity, not by address

Status: accepted · 2026-11-25

## Context

A replica's identity is stored in its data directory. A node whose disk is
wiped or replaced starts under a new identity, at the same network address.
It holds none of the copies the old identity held.

The simulator showed why that is not enough on its own. A writer taking
over a stream looked up the replicas of the old segment, found the address
of one of them, and sent it a fence. The node at that address had been
rebuilt in the meantime. It answered truthfully that it had no copy, and
the recovery counted that answer as if it came from the replica that used to
hold the records. With one more copy unavailable, a committed record was
left out of the recovery end.

## Decision

Nobody connects to an address alone. `ReplicaConnector::connect` takes the
identity of the replica the caller means and its address, and the transport
enforces the pair:

- over gRPC, every request carries the identity in the header
  `bitacora-replica-id`, and the server refuses a request that is not for it
  before the replica sees it;
- in the simulator, the link checks the identity of whatever is running
  behind it when the request arrives.

A refused request is reported as "unavailable", which is the truth: the
replica the caller meant cannot be reached.

## Consequences

- An answer always comes from the replica that was asked, so quorum
  arithmetic counts what it is supposed to count.
- The old identity of a rebuilt node stays in the registry until it has
  been silent long enough to be given up. During that time both identities
  are registered at one address. Placement must not take them for two
  machines, which is what failure-domain labels down to the host are for;
  a deployment that leaves them out loses that protection.
