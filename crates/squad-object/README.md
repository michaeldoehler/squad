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
| `S3Store` | `s3://bucket/prefix?…` | feature `s3` (default). Signature V4, retries with jittered backoff, multipart uploads |
| `CachedStore` | — | wraps any store with a bounded in-memory cache of objects and ranges |

S3 URL parameters: `region`, `endpoint` (for MinIO, Ceph, R2 and other
compatible services), `path_style`, `access_key`, `secret_key`. Missing
values come from `AWS_REGION`, `AWS_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID`
and `AWS_SECRET_ACCESS_KEY`.

## Operations

`put`, `put_if_absent` (atomic create; on S3 a conditional `PUT` with
`If-None-Match: *`), `get`, `get_range`, `head`, `list` (by key prefix,
sorted) and `delete`. Objects are immutable from squad's point of view:
it writes each key once. squad's writer fencing does not use
`put_if_absent`; it needs only `put` and read-after-write consistent
`list`.

## The S3 client

- One HTTP agent per store keeps connections open for reuse. Connect
  and read/write timeouts default to 10 s and 60 s
  (`S3Store::with_timeouts`).
- Failures are classified. Connection errors, timeouts, 5xx, 429, 408
  and S3's `RequestTimeout` are transient and retried, up to 5 attempts
  (`with_retries`), each after a random delay below a bound that
  doubles from 50 ms up to 5 s (full jitter, `with_backoff`). Other 4xx
  answers are permanent and returned at once as `Error::Remote`;
  exhausted retries end in `Error::Transport`.
- Objects above 16 MiB are uploaded in 8 MiB parts (`with_multipart`).
  A failed multipart upload is aborted; a completion whose confirmation
  was lost is recognised on the retry.
- `S3Store::stats` counts requests, retries and multipart uploads.
- `S3Config::from_url` parses the same URLs as `open_url`, for
  adjusting a store before creating it.

## What the S3 client has and has not been tested against

Tested:

- The request signing against the two worked examples in the Amazon S3
  API reference ("Signature Calculations for the Authorization Header")
  and twelve requests of the AWS SigV4 test suite (`get-vanilla`,
  `post-x-www-form-urlencoded` and others whose path needs no
  normalization): the computed signatures are identical.
- Every operation against the S3 emulator in this crate
  (`squad_object::emulator`, feature `test-util`), which re-computes and
  checks the signature of each request, pages list results, honours
  (or, on request, ignores) conditional puts, serves ranges, implements
  multipart uploads, and can fail or drop requests. The same
  conformance suite runs against all stores.
- Retries on 5xx responses and broken connections, including a
  conditional put and a multipart completion whose confirmation is
  lost; no retry of permanent errors.
- **Garage 2.5** (single node, `scripts/garage-dev.sh`): the
  conformance suite including a multipart upload, when
  `SQUAD_TEST_S3_URL` is set. Garage does not enforce
  `If-None-Match: *`; the suite detects that and adjusts.

Not tested:

- Amazon S3 itself, MinIO, Ceph or R2. Run the conformance suite
  against your service before relying on it:
  `SQUAD_TEST_S3_URL='s3://bucket?endpoint=…' cargo test -p squad-object --features test-util --test conformance`.
- TLS endpoints (the emulator and the local Garage speak plain HTTP;
  TLS is handled by `ureq` with rustls).
- Virtual-hosted-style addressing (`bucket.host`); the emulator and the
  Garage tests use path style.

## Limits

- Objects up to the multipart threshold are written with a single
  `PUT`; larger ones in parts, up to 10,000 parts (80 GiB with the
  default part size). The whole object is held in memory.
- No temporary credentials from instance metadata or STS session tokens:
  static access keys only.
- `put_if_absent` is atomic only on a service that implements
  conditional writes (AWS S3 since 2024, MinIO, the emulator). Garage
  accepts the header and overwrites; other services may fail the
  request.
- The blocking HTTP client holds one thread per request.
