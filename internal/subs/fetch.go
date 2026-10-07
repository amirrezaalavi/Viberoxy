// Package subs fetches subscription documents over HTTP.
//
// The fetcher is deliberately strict (review finding F-18):
//
//   - the response body is read through a size cap (MaxBodyBytes); anything
//     larger is an oversized error instead of an unbounded read,
//   - only https:// URLs are fetched unless ALLOW_HTTP_SUBSCRIPTION=true
//     opts in to plain http:// (that env var is validated at startup by
//     proxycfg.ParseConfig),
//   - validators from a previous response (ETag / Last-Modified) are
//     replayed as If-None-Match / If-Modified-Since, and a 304 is a
//     first-class Result — not an error — so callers keep their current
//     configs without re-parsing,
//   - an empty body is an error, so a caller that only replaces configs on
//     a successful fetch keeps its previous ones,
//   - every failure is reported with a distinguishable Kind.
package subs

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"
)

// MaxBodyBytes bounds one subscription response body: 8 MiB. A body larger
// than this is rejected as oversized rather than read to completion.
const MaxBodyBytes int64 = 8 << 20

// Kind classifies a fetch outcome. Not-modified is part of the kind space
// even though it is returned as a Result with a nil error, so a single
// Outcome() call covers every case a caller can observe.
type Kind string

const (
	KindOK          Kind = "ok"
	KindTransport   Kind = "transport"
	KindBadScheme   Kind = "bad_scheme"
	KindOversized   Kind = "oversized"
	KindStatus      Kind = "status"
	KindEmptyBody   Kind = "empty_body"
	KindNotModified Kind = "not_modified"
)

// Error is the error type returned by Fetch. Kind tells the caller what
// went wrong; Err carries the underlying detail.
type Error struct {
	Kind Kind
	URL  string
	Err  error
}

func (e *Error) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("subs: %s %s: %v", e.Kind, e.URL, e.Err)
	}
	return fmt.Sprintf("subs: %s %s", e.Kind, e.URL)
}

func (e *Error) Unwrap() error { return e.Err }

// KindOf reports the Kind of err, or "" when err is nil or was not produced
// by this package.
func KindOf(err error) Kind {
	var e *Error
	if errors.As(err, &e) {
		return e.Kind
	}
	return ""
}

// Outcome classifies a completed fetch — result plus error — into one of
// the Kind values: KindNotModified for a 304, KindOK for a usable body,
// otherwise the Kind of the error.
func Outcome(res Result, err error) Kind {
	if err != nil {
		return KindOf(err)
	}
	if res.NotModified {
		return KindNotModified
	}
	return KindOK
}

// Options tunes one Fetch call.
type Options struct {
	// AllowHTTP additionally permits plain http:// URLs. Independent of
	// this flag, ALLOW_HTTP_SUBSCRIPTION=true in the environment permits
	// them; with neither, only https:// is fetched.
	AllowHTTP bool
	// ETag, when non-empty, is replayed as If-None-Match.
	ETag string
	// LastModified, when non-empty, is replayed as If-Modified-Since.
	LastModified string
	// Client overrides the HTTP client (tests). Defaults to a client with
	// a 30s timeout.
	Client *http.Client
}

// Result is the outcome of a successful Fetch.
type Result struct {
	// Body is the raw response body, at most MaxBodyBytes long.
	Body []byte
	// NotModified reports that the server answered 304: Body is empty and
	// the caller should keep the configs it already has.
	NotModified bool
	// ETag and LastModified are the validators from this response, to be
	// passed to the next conditional Fetch. They are empty when the
	// response carried none (and on a 304, where the caller keeps the
	// validators it already stored).
	ETag         string
	LastModified string
}

// AllowHTTPFromEnv reports whether ALLOW_HTTP_SUBSCRIPTION opts in to plain
// http:// subscription URLs. Unset or unparsable values mean "refuse".
func AllowHTTPFromEnv() bool {
	b, err := strconv.ParseBool(os.Getenv("ALLOW_HTTP_SUBSCRIPTION"))
	return err == nil && b
}

// Fetch downloads rawURL and returns its body, bounded by MaxBodyBytes.
//
// Failures are returned as *Error with a Kind of KindBadScheme (URL not
// https and not opted in), KindTransport (dial/send/read failure),
// KindStatus (non-2xx, non-304 status), KindOversized (body over
// MaxBodyBytes) or KindEmptyBody (no usable payload). A 304 is not a
// failure: it returns Result{NotModified: true} with a nil error.
func Fetch(rawURL string, opts Options) (Result, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return Result{}, &Error{Kind: KindBadScheme, URL: rawURL, Err: err}
	}
	if u.Host == "" {
		return Result{}, &Error{Kind: KindBadScheme, URL: rawURL, Err: errors.New("url has no host")}
	}
	switch u.Scheme {
	case "https":
	case "http":
		if !opts.AllowHTTP && !AllowHTTPFromEnv() {
			return Result{}, &Error{
				Kind: KindBadScheme,
				URL:  rawURL,
				Err:  errors.New("plain http:// subscription refused; set ALLOW_HTTP_SUBSCRIPTION=true to allow it"),
			}
		}
	default:
		return Result{}, &Error{Kind: KindBadScheme, URL: rawURL, Err: fmt.Errorf("unsupported scheme %q", u.Scheme)}
	}

	client := opts.Client
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}

	req, err := http.NewRequest(http.MethodGet, rawURL, nil)
	if err != nil {
		return Result{}, &Error{Kind: KindBadScheme, URL: rawURL, Err: err}
	}
	if opts.ETag != "" {
		req.Header.Set("If-None-Match", opts.ETag)
	}
	if opts.LastModified != "" {
		req.Header.Set("If-Modified-Since", opts.LastModified)
	}

	resp, err := client.Do(req)
	if err != nil {
		return Result{}, &Error{Kind: KindTransport, URL: rawURL, Err: err}
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotModified {
		return Result{NotModified: true}, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return Result{}, &Error{Kind: KindStatus, URL: rawURL, Err: fmt.Errorf("unexpected status %s", resp.Status)}
	}

	// Read one byte past the cap: a body of exactly MaxBodyBytes is fine,
	// anything more is oversized and never buffered in full.
	body, err := io.ReadAll(io.LimitReader(resp.Body, MaxBodyBytes+1))
	if err != nil {
		return Result{}, &Error{Kind: KindTransport, URL: rawURL, Err: err}
	}
	if int64(len(body)) > MaxBodyBytes {
		return Result{}, &Error{Kind: KindOversized, URL: rawURL, Err: fmt.Errorf("body larger than %d bytes", MaxBodyBytes)}
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return Result{}, &Error{Kind: KindEmptyBody, URL: rawURL, Err: errors.New("empty body")}
	}

	return Result{
		Body:         body,
		ETag:         resp.Header.Get("ETag"),
		LastModified: resp.Header.Get("Last-Modified"),
	}, nil
}
