# squad-object

Object storage for the squad database: one small trait, `ObjectStore`,
and implementations for memory, a local directory and S3-compatible
services. squad uses it to keep a complete copy of a database outside
the machine it runs on; see [docs/cloud-storage.md](../../docs/cloud-storage.md).

```rust
use squad_object::{open_url, ObjectStore};

let store = open_url("s3://my-bucket/databases/app?region=eu-central-1")?;
store.put("hello/world", b"data")?;
assert_eq!(store.get("hello/world")?.as_deref(), Some(&b"data"[..]));
for object in store.list("hello/")? {
    println!("{} ({} bytes)", object.key, object.size);
}
```

| Store | URL | Notes |
| --- | --- | --- |
| `MemoryStore` | `memory://` | volatile; can inject faults for tests |
| `LocalStore` | `file:///path` | one file per object; a put is temp file + `fsync` + rename + directory `fsync` |
| `S3Store` | `s3://bucket/prefix?…` | feature `s3` (default). Signature V4, retries with backoff |
| `CachedStore` | — | wraps any store with a bounded in-memory cache of objects and ranges |

S3 URL parameters: `region`, `endpoint` (for MinIO, Ceph, R2 and other
compatible services), `path_style`, `access_key`, `secret_key`. Missing
values come from `AWS_REGION`, `AWS_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID`
and `AWS_SECRET_ACCESS_KEY`.

## Operations

`put`, `put_if_absent` (atomic create; on S3 a conditional `PUT` with
`If-None-Match: *`), `get`, `get_range`, `head`, `list` (by key prefix,
sorted) and `delete`. Objects are immutable from squad's point of view:
it writes each key once.

## What the S3 client has and has not been tested against

Tested:

- The request signing against the two worked examples in the Amazon S3
  API reference ("Signature Calculations for the Authorization Header"):
  the computed signatures are identical.
- Every operation against the S3 emulator in this crate
  (`squad_object::emulator`, feature `test-util`), which re-computes and
  checks the signature of each request, pages list results, honours
  conditional puts and ranges, and can fail or drop requests. The same
  conformance suite runs against all stores.
- Retries on 5xx responses and broken connections, including a
  conditional put whose confirmation is lost.

Not tested:

- **A real S3 service or MinIO.** None was reachable from the
  environment this was developed in. The emulator and the client were
  written by the same hand, so a shared misunderstanding of the protocol
  would not be caught, beyond what the published signing examples pin
  down. Before relying on it, run the conformance suite against your
  service.
- TLS endpoints (the emulator speaks plain HTTP; TLS is handled by
  `ureq` with rustls).
- Virtual-hosted-style addressing (`bucket.host`); the emulator uses
  path style.

## Limits

- Objects are written with a single `PUT` (S3 allows up to 5 GiB).
  There is no multipart upload; squad writes objects of at most a few
  megabytes.
- No temporary credentials from instance metadata or STS session tokens:
  static access keys only.
- `put_if_absent` needs a service that implements conditional writes
  (AWS S3 since 2024, MinIO, the emulator). Elsewhere the request either
  fails or overwrites, depending on the service.
- The blocking HTTP client holds one thread per request.
