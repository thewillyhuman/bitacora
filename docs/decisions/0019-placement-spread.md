# 0019. Placement spreads over replicas that are equally good

Status: accepted · 2026-11-26

## Context

The design asks placement to choose replica sets that "span failure domains
and balance free space" (section 8). Placement did that literally: one copy
at a time, the replica that shares the fewest failure domains with the
copies already placed, then the one with the most free bytes, then the
lowest identifier.

On the first cluster on real machines, five replicas in three zones of one,
two and two, a benchmark created 64 streams and later 128. Every one of them
got its first segment on the same three replicas, one per zone, and the two
other replicas stayed idle. Free bytes differ a little between any two
replicas and change slowly, so for as long as one replica of a zone was a
few megabytes emptier than its neighbour, it won every placement.

That costs more than balance. On that cluster's volumes a replica completes
about 600 writes a second in total, shared by every stream it hosts
([measurements](../measurements.md)). Putting all streams on half of the
replicas doubles how long each append waits, for nothing. The byte-exact
tie-break balances space in the long run and concentrates load at every
moment.

## Decision

Placement compares candidates for the next copy by four criteria, in this
order:

1. **Spread over failure domains**, unchanged: the replica that shares the
   fewest domains with the copies already placed, widest domain first.
2. **Room, by tenths of the disk in use.** A replica that is a tenth emptier
   wins. Replicas in the same tenth are equals.
3. **A placement key.** Among equals, the replica with the highest hash of
   the key and its own identifier wins: rendezvous hashing. The key says
   what is being placed. For a segment of a stream it is derived from the
   stream's identifier and the segment's, so the successive segments of one
   stream move around as well as different streams do. For the first
   segment of a stream that has no identifier yet, it is derived from the
   stream's name.
4. **The identifier**, lowest first, if two hashes are the same number.

Placement stays a pure function: the key is an argument, and the same
registry, time, policy, exclusions and key give the same replicas in the
same order, in every program.

The hash is fixed by the code and held by tests, because two programs that
place the same segment must agree whatever Rust version built them. A key is
the 64-bit FNV-1a hash of its input: the stream's identifier and the
segment's as eight bytes each, least significant first, or the UTF-8 bytes
of the stream's name. A replica's score is FNV-1a over the eight bytes of
the key and then the bytes of its identifier, put through the final mixing
step of the Murmur 3 hash. FNV-1a alone is not enough for the score: it
changes its high bits little for a change in the last byte, and with five
replicas whose identifiers differ in their last character it gave one of
them half of all keys.

Who passes which key:

| Caller | Key |
| --- | --- |
| The controller, placing a stream's standby | The stream's identifier and its next segment's |
| The controller, replacing a lost copy | The stream's identifier and the segment's |
| The administration client, placing the first segment of a new stream | The stream's name |

The controller still replaces a standby only when a set chosen now would
span more failure domains. A set that merely names other replicas of the
same spread, because the key or the room changed, is not a reason.

## Not done

- **Load reported by replicas.** A replica could report how busy it is, and
  placement could prefer the idle ones. That balances what actually costs,
  but it makes placement depend on a number that changes every second, and
  two programs would no longer agree on a choice. Spreading by key needs
  nothing reported and is the same everywhere.
- **Moving segments.** A segment stays where it was placed. Streams placed
  before this change stay on the replicas they have until they roll.
- **A finer measure of room.** Tenths are coarse: a replica at 9% wins every
  placement over one at 11% until it has taken one more percent of its
  disk. That is the old behaviour in a small window, and it ends by itself.

## Consequences

- A replica is no longer preferred for being a little emptier. New replicas
  still fill before old ones grow, once the difference is a tenth of a disk.
- Streams created together, the standbys of different streams and the
  copies rebuilt after a replica is lost are each spread over the replicas
  that are equally good. With 1,000 keys and two equal replicas the split is
  476 to 524; with five, each gets between 185 and 218.
- Which replica of a zone holds a copy can no longer be read off the
  registry by a person. `bita segment list` says where a segment is.
- Tests that relied on the old tie-break name the replicas they need, or
  assert the zones a set covers.
- Every history of the simulator changed, since placement decides who holds
  what. The fault matrix passes for 240 seeds with the regression seeds. A
  deliberately broken store is caught by more seeds than before (15 of 48,
  from 7), but not by the same ones, so its test now tries sixteen seeds
  where it tried four.
- Another program that creates streams, in another language, is free to
  choose differently among equals; the protocol only asks that it does not
  put two copies in one narrowest failure domain.
