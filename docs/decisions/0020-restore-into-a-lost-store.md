# 0020. A restore keeps the registrations of running replicas

Status: accepted · 2026-11-26

## Context

`bita metadata restore` refused a metadata store that held any key, because
writing a snapshot into a store in use would mix two states
([0007](0007-operator-tooling.md)). `--force` restored anyway and made the
store exactly what the snapshot was.

The procedure for a lost metadata store stops the writers and the
controllers and leaves the replicas running: they hold the data, and the
rebuild that follows asks them for it. A replica that is running renews its
registration every few seconds, and a renewal is an unconditional write. So
a lost store holds one key per running replica within seconds of being
lost.

The first restore drill on real machines met exactly that. The store had
been emptied and held five keys by the time the restore ran, the
registrations of the five replicas, and the restore was refused with "the
metadata store holds 5 keys". The refusal did not say what the keys were.
The only way on was `--force`.

A safeguard that every correct use has to override protects nothing. An
operator who has learnt that a restore always needs `--force` passes it
against the wrong cluster too.

## Decision

A restore that is not forced accepts a store that holds registrations of
replicas and nothing else, and treats what it finds there as follows:

- A registration that is in the store stays as it is. A registration is the
  replica's own statement, and the one it wrote after the loss is newer
  than the one in the snapshot.
- Every other key of the snapshot is written, the registrations of replicas
  that have not come back among them. Such a replica is still part of the
  cluster: the rebuild asks every registered replica for its copies and
  must find out that this one does not answer.
- A key of the snapshot is written only if it is still absent, as before.
  A registration is the exception: a replica that was down may register
  at any moment, a condition on its key would stop the restore at random
  and half done, and the replica's next renewal replaces what was restored
  anyway.

A store that holds any other key is in use and is refused as before. The
refusal counts the keys that are not registrations. `--force` is unchanged:
the store ends up exactly as the snapshot was, registrations included.

The rule lives in the metadata store (`RestoreMode::IntoLostStore`). The
tool asks the store for a preview of the restore and reads the answer, so
that a dry run, the refusal and the restore itself cannot disagree. The
report gained a count of the keys that were kept.

## Alternatives

- **Document that a restore needs `--force`.** No code, and the safeguard
  is gone in practice, as above.
- **Stop the replicas for the restore.** The store would stay empty, at the
  price of a longer outage and of a procedure in which the rebuild, the
  step right after, needs the replicas up again.
- **Write the snapshot's registrations over those in the store.** Simpler
  to state, and wrong for a few seconds: a running replica would be
  recorded with a heartbeat minutes old until its next renewal.

## Consequences

- The documented procedure works as written: `bita metadata restore
  latest`, then `bita metadata rebuild`.
- A store that holds only registrations is restored into without force
  even if it was never lost, for example a new cluster whose replicas have
  started and that has no stream yet. Nothing is overwritten or deleted
  there either.
- The in-process test cluster now has a way to lose its metadata store the
  way a real one does, with the replicas registering again. The tests that
  lost it without that could not see this defect.
