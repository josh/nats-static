# nats-static

Serve static files straight out of a [NATS JetStream object store](https://docs.nats.io/nats-concepts/jetstream/obj_store) over HTTP.

`nats-static` opens a single object store bucket and:

- **`GET` / `HEAD`** any key over HTTP. The request path maps to a flat object key
  (cleaned, leading slash stripped; an empty path serves `index.html`). The response
  `Content-Type` comes from the object's stored `content-type` metadata, falling back to the
  file extension, then `application/octet-stream`. `ETag` (object digest) and `Last-Modified` are
  set, and conditional requests (`If-None-Match`, `If-Modified-Since`) return `304`; `Range`
  requests return `206`. Writers can attach extra response headers — see [Writing objects](#writing-objects).
- **`static.put.obj`** (NATS request): store an object in one message. Headers: `Path` (required)
  and optional `Content-Type`; the body is the content. Replies `OK size=… digest=…`.
- **`static.put.begin`** (NATS request): open a multipart session for objects larger than the NATS
  max payload — see [Writing objects](#writing-objects).
- **`static.delete`** (NATS request): remove an object by `Path`. Replies `OK`.

Writes can be scoped to a path prefix by encoding it in the subject (`static.put.obj.blog`,
`static.put.begin.blog`, `static.delete.blog`, …) — see [Writing objects](#writing-objects).

The object store bucket is **never created** by the server — it is provisioned out of band (by
[NACK](https://github.com/nats-io/nack), the chart's `nack.enabled` ObjectStore CRD, or any other
means). On boot the server retries opening the bucket until it exists.

## Configuration

Config is a JSON file; its path comes from `NATS_STATIC_CONFIG` (default
`/etc/nats-static/config.json`):

```json
{
  "nats": {
    "url": "nats://nats:4222",
    "user_file": "...",
    "password_file": "..."
  },
  "object_store": { "bucket": "static" },
  "http": { "listen": ":8080" }
}
```

NATS auth is selected by whichever credential file is set: `user_file`+`password_file`,
`token_file`, `creds_file`, or `nkey_seed_file`. With none set the connection is anonymous. The
files hold the credential material (mounted from a Kubernetes Secret by the chart).

`nats-static version` prints the version and exits.

## Helm chart

A chart lives in [`charts/nats-static`](charts/nats-static). It renders the Deployment, Service,
ConfigMap, and (optionally, via `nack.enabled`) the NACK `ObjectStore` and `Account` CRDs. It does
**not** render an Ingress — point your own Ingress at the Service's `http` port. See
[`values.yaml`](charts/nats-static/values.yaml) for the auth/secret and NACK options.

## Container image

Prebuilt images are published to `ghcr.io/josh/nats-static`.

## Writing objects

Write verbs live under `static.put.*` plus `static.delete`. Each is addressable **bare**
(unrestricted) or **prefix-scoped** by appending the prefix as subject tokens (`.` → `/`) — e.g.
`static.put.obj.blog.images` confines writes to `blog/images/`. The server rejects any `Path`
outside the subject's prefix (`ERR path outside permitted prefix`).

**Single-shot — `static.put.obj[.<prefix>]`.** One request carries the whole object (≤ NATS
`max_payload`, 1 MiB default). Headers: `Path` (required), optional `Content-Type`. Replies
`OK size=… digest=…`.

**Multipart — `static.put.begin[.<prefix>]`.** For larger objects, a request/reply protocol streams
chunks into the store with bounded memory:

1. `static.put.begin` with `Path` (optional `Content-Type`, `Size`, `Digest`) → reply
   `OK session=<id> subject=static.put.session.<id> max_chunk=<bytes> mode=acked`. The session is
   pinned to the replica that answered.
2. Send chunks to the session subject with a monotonic `Seq` (from `0`, body ≤ `max_chunk`); each is
   acked `OK seq=<n>`. Any gap/duplicate/out-of-order `Seq` aborts the upload.
3. Finish with an `EOF: true` request (empty body) → `OK size=<n> digest=<SHA-256=…>`. A
   `Size`/`Digest` declared at begin that doesn't match rolls the object back with an `ERR`.

**Response headers.** Uploads may carry an allowlist — `Cache-Control`, `Content-Disposition`,
`Content-Encoding`, `Content-Language`, `Vary` (plus `Content-Type`) — stored on the object and
replayed on `GET`/`HEAD`. Other headers are ignored.

**Prefix scoping.** Because the boundary is in the subject, a writer can be delegated a path
namespace via NATS export/import — grant `static.put.obj.<prefix>` (and `.>`),
`static.put.begin.<prefix>` (and `.>`), and `static.put.session.>`, and it can write only under that
prefix. Bare `static.put.*` stays unrestricted; keep `static.delete` unexported to make a writer's
access append-only.
