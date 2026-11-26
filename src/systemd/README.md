# systemd units

What a machine needs to run the bitacora daemons under systemd: on virtual machines, or on bare metal
managed with Puppet.

| File | Installed as | What it is |
| --- | --- | --- |
| `bita-replica.service` | `/usr/lib/systemd/system/bita-replica.service` | The unit of the replica |
| `bita-controller.service` | `/usr/lib/systemd/system/bita-controller.service` | The unit of the controller |
| `bitacora.sysusers` | `/usr/lib/sysusers.d/bitacora.conf` | The `bitacora` user and group the daemons run as |
| `package.sh` | not installed | Builds the archive below; called by Bazel |

## The archive

```sh
bazel build --config=release //src/systemd:package
```

writes `bazel-bin/src/systemd/bitacora-systemd.tar.gz`, for the kind of machine Bazel ran on. It holds
only files, owned by root, at the paths they are installed to:

```text
usr/bin/bita-replica
usr/bin/bita-controller
usr/bin/bita
usr/lib/systemd/system/bita-replica.service
usr/lib/systemd/system/bita-controller.service
usr/lib/sysusers.d/bitacora.conf
etc/bitacora/replica.toml.example
etc/bitacora/controller.toml.example
```

The two `.example` files are the examples each daemon keeps next to its code, copied by the build, so
they always describe the settings of the programs in the same archive. The archive never holds
`replica.toml` or `controller.toml` themselves: those belong to the operator, and unpacking a newer
archive must not overwrite them.

## Installing by hand

On each machine, as root:

```sh
tar -C / -xzf bitacora-systemd.tar.gz
systemd-sysusers                       # creates the bitacora user
systemctl daemon-reload
```

On a machine that is a replica:

```sh
cp /etc/bitacora/replica.toml.example /etc/bitacora/replica.toml
$EDITOR /etc/bitacora/replica.toml     # replica_id, advertised_address, failure_domains, [cluster]
bita-replica --config /etc/bitacora/replica.toml --check-config
systemctl enable --now bita-replica
```

On a machine that is a controller (two or three of them, each with its own `identity`):

```sh
cp /etc/bitacora/controller.toml.example /etc/bitacora/controller.toml
$EDITOR /etc/bitacora/controller.toml  # identity, [cluster]
bita-controller --config /etc/bitacora/controller.toml --check-config
systemctl enable --now bita-controller
```

A daemon that started is not yet a daemon that works. Ask it:

```sh
curl -fsS http://127.0.0.1:9100/readyz   # the replica: registered in the cluster
curl -fsS http://127.0.0.1:9101/readyz   # the controller: completed a round
bita --etcd-endpoint http://10.0.0.1:2379 replica list
```

## Installing with Puppet

The archive maps onto four resources per role. This sketch is not run by any test of this repository;
it shows the order that matters, not a module to copy.

```puppet
exec { 'bitacora-user':
  command     => '/usr/bin/systemd-sysusers /usr/lib/sysusers.d/bitacora.conf',
  refreshonly => true,
  subscribe   => File['/usr/lib/sysusers.d/bitacora.conf'],
}

file { '/etc/bitacora/replica.toml':
  owner        => 'root',
  group        => 'bitacora',
  mode         => '0640',
  content      => epp('bitacora/replica.toml.epp'),
  validate_cmd => '/usr/bin/bita-replica --config % --check-config',
  require      => Exec['bitacora-user'],
  notify       => Service['bita-replica'],
}

service { 'bita-replica':
  ensure => running,
  enable => true,
}
```

`validate_cmd` is the reason `--check-config` exists: Puppet writes the new file only if the replica
accepts it, so a mistake in Hiera never reaches a running replica. Restart replicas one at a time: a
`notify` that restarts every replica of a cluster in the same Puppet run takes the cluster below its
write quorum.

## What an operator must know about the units

- **Exit codes.** A daemon that exits with 1 could not start or had to stop: systemd restarts it after
  two seconds. A daemon that exits with 2 was given a wrong command line or configuration: systemd
  leaves it stopped (`RestartPreventExitStatus=2`), because restarting cannot fix a file. `systemctl
  status` and the journal say what is wrong.
- **Stopping.** `systemctl stop` sends SIGTERM. The daemons finish what is in flight within ten
  seconds; the units wait thirty before systemd kills them.
- **Where a daemon may write.** Only `/var/lib/bitacora`, which systemd creates for the `bitacora`
  user; the rest of the file system is read-only to the daemons (`ProtectSystem=strict`). A replica
  whose `data_directory` is elsewhere, such as a disk of its own, needs a drop-in:

  ```ini
  # /etc/systemd/system/bita-replica.service.d/data.conf
  [Service]
  ReadWritePaths=/srv/bitacora
  ```

  and that directory must belong to the `bitacora` user.
- **Variables and credentials.** A unit reads `/etc/bitacora/bita-replica.env` or
  `/etc/bitacora/bita-controller.env` if the file exists. Use it for values the configuration file
  refers to as `"${NAME}"`, and for the credentials of an S3 object store (`AWS_ACCESS_KEY_ID`,
  `AWS_SECRET_ACCESS_KEY`), which never go in the configuration file. systemd reads the file as root,
  so make it `root:root` with mode `0600`.
- **Open files.** A replica keeps a file open per segment it writes and a connection per client, so
  its unit raises the limit to 1048576.
- **One replica per machine.** The unit runs one replica with one data directory. Several replicas on
  one machine would share a failure domain, which defeats what they are for.

## How this directory is tested

- `//src/systemd:units_test` builds the archive, unpacks it, and checks each unit against what it is
  installed with: every section and directive is one the checker knows (a misspelled directive is
  otherwise only a warning in the journal and a protection that silently does not apply); the program
  in `ExecStart` is in the archive and has every option it is given, according to its own `--help`;
  the configuration it names has an example in the archive that the program accepts with
  `--check-config`; every directory that example writes to is inside the unit's `StateDirectory`; the
  user exists in the sysusers file and is not root; exit code 2 does not restart; the stop timeout is
  longer than the daemons' shutdown; the protections are on. Where `systemd-analyze` is installed (on
  Linux), `systemd-analyze verify` also reads every unit and anything it says fails the test; the
  last line of the test log says whether it ran.
- `//src/systemd:units_checker_test` hands the same checker a unit with seven known mistakes and
  requires it to fail and to name each one, so that the checker cannot rot into one that passes
  everything.
- `//src/systemd:shellcheck_test` lints the scripts.

Each takes under a second once the programs are built. None of them starts a daemon under systemd:
that needs a machine with systemd as its first process, which a hermetic test does not have.
