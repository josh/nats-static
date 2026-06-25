package main

import (
	"bytes"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	queueGroup = "static-server"

	// Base subjects for each write verb. A write may target the bare subject
	// (unrestricted) or a prefix-scoped form "<base>.<prefix tokens>" where the
	// trailing tokens encode a path prefix the write must stay within. Encoding
	// the prefix in the subject lets NATS account exports delegate writes to a
	// path namespace; the server enforces the same boundary as defense-in-depth.
	// All single/multipart put verbs live under static.put.* (see multipart.go).
	putSubject    = "static.put.obj"
	deleteSubject = "static.delete"
)

// server serves objects from a NATS object store over HTTP (GET/HEAD) and
// handles the static.put / static.delete NATS requests.
type server struct {
	obs nats.ObjectStore
}

// subjectPrefix extracts the path prefix encoded in the tokens after base,
// mapping subject dots to path slashes. It returns "" for the bare subject
// (unrestricted). Example: ("static.put.blog.images", "static.put") -> "blog/images".
func subjectPrefix(subject, base string) string {
	rest := strings.TrimPrefix(strings.TrimPrefix(subject, base), ".")
	return strings.ReplaceAll(rest, ".", "/")
}

// withinPrefix reports whether key is permitted under prefix: the empty prefix
// permits everything, otherwise key must equal the prefix or be nested under it.
func withinPrefix(key, prefix string) bool {
	return prefix == "" || key == prefix || strings.HasPrefix(key, prefix+"/")
}

// normalizeKey maps a request path or Path header to a flat object-store key:
// cleaned, no leading slash. An empty result becomes index.html.
func normalizeKey(raw string) string {
	key := strings.TrimPrefix(path.Clean("/"+raw), "/")
	if key == "" {
		key = "index.html"
	}
	return key
}

// headerMetaPrefix namespaces stored response-header values within an object's
// metadata so they can be replayed on GET without colliding with other metadata.
const headerMetaPrefix = "header:"

// responseHeaders is the allowlist of response headers a writer may attach to an
// object at upload time (stored on the object, replayed on GET). Content-Type is
// handled separately since it also drives the served Content-Type.
var responseHeaders = []string{
	"Cache-Control",
	"Content-Disposition",
	"Content-Encoding",
	"Content-Language",
	"Vary",
}

// uploadMetadata builds an object's metadata from an upload request's headers:
// the content-type (existing convention) plus any allowlisted response headers
// under headerMetaPrefix. Returns nil when nothing is set.
func uploadMetadata(h nats.Header) map[string]string {
	meta := map[string]string{}
	if ct := h.Get("Content-Type"); ct != "" {
		meta["content-type"] = ct
	}
	for _, name := range responseHeaders {
		if v := h.Get(name); v != "" {
			meta[headerMetaPrefix+name] = v
		}
	}
	if len(meta) == 0 {
		return nil
	}
	return meta
}

func contentTypeFor(key string, info *nats.ObjectInfo) string {
	if info != nil {
		if ct := info.Metadata["content-type"]; ct != "" {
			return ct
		}
	}
	if ct := mime.TypeByExtension(path.Ext(key)); ct != "" {
		return ct
	}
	return "application/octet-stream"
}

func (s *server) put(msg *nats.Msg) {
	key := msg.Header.Get("Path")
	if key == "" {
		msg.Respond([]byte("ERR missing Path header"))
		return
	}
	key = normalizeKey(key)
	if !withinPrefix(key, subjectPrefix(msg.Subject, putSubject)) {
		msg.Respond([]byte("ERR path outside permitted prefix"))
		return
	}
	meta := &nats.ObjectMeta{Name: key, Metadata: uploadMetadata(msg.Header)}
	info, err := s.obs.Put(meta, bytes.NewReader(msg.Data))
	if err != nil {
		msg.Respond([]byte("ERR " + err.Error()))
		return
	}
	msg.Respond([]byte(fmt.Sprintf("OK size=%d digest=%s", info.Size, info.Digest)))
}

func (s *server) del(msg *nats.Msg) {
	key := msg.Header.Get("Path")
	if key == "" {
		msg.Respond([]byte("ERR missing Path header"))
		return
	}
	key = normalizeKey(key)
	if !withinPrefix(key, subjectPrefix(msg.Subject, deleteSubject)) {
		msg.Respond([]byte("ERR path outside permitted prefix"))
		return
	}
	if err := s.obs.Delete(key); err != nil && !errors.Is(err, nats.ErrObjectNotFound) {
		msg.Respond([]byte("ERR " + err.Error()))
		return
	}
	msg.Respond([]byte("OK"))
}

func (s *server) mux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.serveHTTP)
	return mux
}

func (s *server) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	key := normalizeKey(r.URL.Path)

	info, err := s.obs.GetInfo(key)
	if err != nil {
		if errors.Is(err, nats.ErrObjectNotFound) {
			http.NotFound(w, r)
			return
		}
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	h := w.Header()
	h.Set("Content-Type", contentTypeFor(key, info))
	var etag string
	if info.Digest != "" {
		etag = `"` + info.Digest + `"`
		h.Set("ETag", etag)
	}
	if !info.ModTime.IsZero() {
		h.Set("Last-Modified", info.ModTime.UTC().Format(http.TimeFormat))
	}
	// Replay any response headers the writer stored on the object.
	for k, v := range info.Metadata {
		if name, ok := strings.CutPrefix(k, headerMetaPrefix); ok {
			h.Set(name, v)
		}
	}

	// Honor conditional requests before fetching the body so 304s stay cheap.
	if notModified(r, etag, info.ModTime) {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	if r.Method == http.MethodHead {
		h.Set("Content-Length", strconv.FormatUint(info.Size, 10))
		w.WriteHeader(http.StatusOK)
		return
	}
	data, err := s.obs.GetBytes(key)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	// ServeContent adds Range support + Content-Length and respects the
	// Content-Type/ETag/Last-Modified already set above.
	http.ServeContent(w, r, key, info.ModTime, bytes.NewReader(data))
}

// notModified reports whether a conditional GET/HEAD may be answered with 304.
// If-None-Match takes precedence over If-Modified-Since (RFC 7232).
func notModified(r *http.Request, etag string, mod time.Time) bool {
	if inm := r.Header.Get("If-None-Match"); inm != "" {
		return inm == "*" || etagMatches(inm, etag)
	}
	if ims := r.Header.Get("If-Modified-Since"); ims != "" && !mod.IsZero() {
		if t, err := http.ParseTime(ims); err == nil && !mod.Truncate(time.Second).After(t) {
			return true
		}
	}
	return false
}

// etagMatches reports whether the strong etag appears in a (possibly multi-value,
// weakly-tagged) If-None-Match header.
func etagMatches(ifNoneMatch, etag string) bool {
	if etag == "" {
		return false
	}
	for _, c := range strings.Split(ifNoneMatch, ",") {
		if strings.TrimPrefix(strings.TrimSpace(c), "W/") == etag {
			return true
		}
	}
	return false
}
