# Reading catalog-managed tables

To read a catalog-managed table, you provide Kernel with a complete catalog observation:
the latest ratified version and every commit reference the catalog retains. Kernel combines
these with published log files to build a [Snapshot](../concepts/architecture.md).

Before reading this page, make sure you understand
[Catalog-managed tables](./overview.md).

## Table resolution is outside Kernel

Kernel doesn't know about table names, table IDs, or catalog APIs. Before you can use
any Kernel API, the catalog client must:

1. **Resolve the table name to a storage path** (e.g. `unity.my_schema.my_table` ->
   `s3://bucket/path/to/table/`)
2. **Fetch storage credentials** (e.g. temporary AWS credentials for the table's S3
   location)
3. **Get the complete retained commit list and latest ratified version** in one catalog observation

Each catalog has its own APIs for these steps.

## Why a log tail?

A catalog-managed table can have a mix of published commits (already in `_delta_log/`)
and unpublished ratified commits (in `_staged_commits/` or inline in the catalog).
Kernel lists `_delta_log/` to find published commits and checkpoints, but it cannot
discover unpublished commits on its own. The **log tail** is the catalog's retained commit list
that supplies those references. A complete list can omit an already-published prefix.

For example, suppose versions 0-7 are published and versions 8-9 are staged:

```text
_delta_log/
  00000000000000000000.json          (published)
  ...
  00000000000000000007.json          (published)
  _staged_commits/
    00000000000000000008.<uuid>.json  (ratified, not published)
    00000000000000000009.<uuid>.json  (ratified, not published)
```

Without a log tail, Kernel would only see versions 0-7. The log tail tells Kernel about
versions 8 and 9.

## Building a log tail

The catalog client fetches the complete retained commit list, then translates every entry into a
`LogPath`. Verify completeness from your serving API's contract and request coverage. Don't filter
out published filenames or limit the response to versions after a cached snapshot.

```rust,ignore
use delta_kernel::LogPath;

let log_path = LogPath::from_commit_filename(
    table_url.clone(),                              // table root URL
    8,                                              // version the catalog reported
    "00000000000000000008.<uuid>.json",             // filename the catalog reported
    1234567890,                                     // last modified timestamp
    4096,                                           // file size in bytes
)?;
```

A staged commit filename follows the Delta staged-commit convention: a zero-padded 20-digit
version, the UUID assigned when the commit was staged, and a `.json` extension. Kernel mints the
UUID inside `CommitMetadata::staged_commit_path()` at write time; the writer passes the
resulting path to the catalog, and the catalog returns that same path on reads. If your
catalog returns commits as `(version, uuid, ...)` tuples, format the filename as
`{version:020}.{uuid}.json`. Catalogs may also report a commit that is already published by its
`{version:020}.json` filename; `LogPath::from_commit_filename` resolves either form to the
right directory. It returns an error if the filename's version does not match `version`.

Then pass the resulting `Vec<LogPath>` to `SnapshotBuilder` along with the maximum
catalog-ratified version:

```rust,ignore
use delta_kernel::Snapshot;

let snapshot = Snapshot::builder_for(table_url)
    .with_catalog_metadata(latest_version, log_paths)
    .build(&engine)?;
```

For the full input contract, see
[`SnapshotBuilder::with_catalog_metadata`](https://docs.rs/delta_kernel/latest/delta_kernel/snapshot/struct.SnapshotBuilder.html#method.with_catalog_metadata).
A nonempty list must be ascending, contiguous, and end at `latest_version`, including when you
request an earlier snapshot. An empty complete list means every ratified commit through
`latest_version` is published.

The log tail can overlap with published commits already in `_delta_log/`. For any
version present in both, Kernel uses the log-tail entry, as the Delta spec requires
readers to prefer the catalog-supplied commit when the catalog provides one.

`latest_version` bounds the snapshot to what the catalog has ratified, even when storage lists
newer published files.

> [!NOTE]
> To time-travel to a specific version within a catalog-managed table, call both
> `.at_version(v)` and `.with_catalog_metadata(N, complete_commits)` on the builder. Call order
> doesn't matter. The requested version must not exceed `N`. Keep the complete response even
> when `v` is older than the catalog's latest version.

## Refreshing a cached snapshot

Pass a fresh complete observation to an incremental builder even when the table version hasn't
advanced:

```rust,ignore
let refreshed = Snapshot::builder_from(cached_snapshot)
    .with_catalog_metadata(latest_version, complete_commits)
    .build(&engine)?;
```

Kernel replaces cached staged references that the catalog has retired with published paths.
Install the returned snapshot in your cache; other holders of the original snapshot keep its
original references. When Kernel doesn't have observed published metadata, replacement paths
inherit the staged file's size and modification time. That time doesn't measure the published
object's age.

The deprecated `with_log_tail` and `with_max_catalog_version` methods preserve partial-tail and
bound-only inputs. Those inputs don't establish retirement of omitted references. Don't combine
them with `with_catalog_metadata` or relabel a filtered tail as a complete response.

For a complete Unity Catalog example, see
[Reading Unity Catalog tables](../unity_catalog/reading.md).

After building the snapshot, reading works the same as for a filesystem-managed table:
build a scan, apply predicates, and read data. See [Building a scan](../reading/building_a_scan.md).

## What's next

- [Writing: the commit and publish flow](./writing.md) covers how writes work for
  catalog-managed tables.
- [Implementing a catalog committer](./committer.md) describes the `Committer` trait in
  detail.
