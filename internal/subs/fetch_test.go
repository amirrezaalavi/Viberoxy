package subs

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
)

// unsetAllowHTTP removes ALLOW_HTTP_SUBSCRIPTION for the duration of the
// test so the https-only default is exercised even if the ambient
// environment opts in.
func unsetAllowHTTP(t *testing.T) {
	t.Helper()
	orig, ok := os.LookupEnv("ALLOW_HTTP_SUBSCRIPTION")
	os.Unsetenv("ALLOW_HTTP_SUBSCRIPTION")
	t.Cleanup(func() {
		if ok {
			os.Setenv("ALLOW_HTTP_SUBSCRIPTION", orig)
		} else {
			os.Unsetenv("ALLOW_HTTP_SUBSCRIPTION")
		}
	})
}

// setAllowHTTP sets ALLOW_HTTP_SUBSCRIPTION for the duration of the test.
func setAllowHTTP(t *testing.T, value string) {
	t.Helper()
	orig, ok := os.LookupEnv("ALLOW_HTTP_SUBSCRIPTION")
	os.Setenv("ALLOW_HTTP_SUBSCRIPTION", value)
	t.Cleanup(func() {
		if ok {
			os.Setenv("ALLOW_HTTP_SUBSCRIPTION", orig)
		} else {
			os.Unsetenv("ALLOW_HTTP_SUBSCRIPTION")
		}
	})
}

// TestFetchHTTPRejectedByDefault: a plain http:// subscription URL must be
// refused with a bad_scheme error unless ALLOW_HTTP_SUBSCRIPTION opts in.
func TestFetchHTTPRejectedByDefault(t *testing.T) {
	unsetAllowHTTP(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ss://whatever@1.2.3.4:12345#x")
	}))
	defer srv.Close()

	res, err := Fetch(srv.URL, Options{})
	if err == nil {
		t.Fatalf("Fetch(%q) with default policy: expected error, got body %q", srv.URL, res.Body)
	}
	if got := KindOf(err); got != KindBadScheme {
		t.Errorf("KindOf(err) = %q, want %q (err: %v)", got, KindBadScheme, err)
	}
	if Outcome(res, err) != KindBadScheme {
		t.Errorf("Outcome = %q, want %q", Outcome(res, err), KindBadScheme)
	}
}

// TestFetchHTTPAllowedWhenEnvSet: ALLOW_HTTP_SUBSCRIPTION=true lifts the
// scheme policy and the fetch succeeds, returning the raw body bytes.
func TestFetchHTTPAllowedWhenEnvSet(t *testing.T) {
	setAllowHTTP(t, "true")

	body := "ss://YWVzLTEyOC1nY206cGFzc3dvcmQ=@1.2.3.4:12345#TestSS\n" +
		"trojan://password123@5.6.7.8:443#TestTrojan"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, body)
	}))
	defer srv.Close()

	res, err := Fetch(srv.URL, Options{})
	if err != nil {
		t.Fatalf("Fetch(%q) with ALLOW_HTTP_SUBSCRIPTION=true: %v", srv.URL, err)
	}
	if res.NotModified {
		t.Error("NotModified = true, want false for a fresh 200 response")
	}
	if string(res.Body) != body {
		t.Errorf("Body = %q, want %q", res.Body, body)
	}
	if Outcome(res, nil) != KindOK {
		t.Errorf("Outcome = %q, want %q", Outcome(res, nil), KindOK)
	}
}

// TestFetchOversizedBody: a body larger than MaxBodyBytes (8 MiB) must be
// an oversized error, never an unbounded read.
func TestFetchOversizedBody(t *testing.T) {
	big := bytes.Repeat([]byte("a"), 9<<20) // 9 MiB > 8 MiB cap
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprint(len(big)))
		_, _ = w.Write(big)
	}))
	defer srv.Close()

	res, err := Fetch(srv.URL, Options{AllowHTTP: true})
	if err == nil {
		t.Fatalf("Fetch of 9 MiB body: expected error, got %d bytes", len(res.Body))
	}
	if got := KindOf(err); got != KindOversized {
		t.Errorf("KindOf(err) = %q, want %q (err: %v)", got, KindOversized, err)
	}
}

// TestFetchBodyAtCapAccepted: a body of exactly MaxBodyBytes is within the
// policy and must be returned intact.
func TestFetchBodyAtCapAccepted(t *testing.T) {
	atCap := bytes.Repeat([]byte("b"), int(MaxBodyBytes))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(atCap)
	}))
	defer srv.Close()

	res, err := Fetch(srv.URL, Options{AllowHTTP: true})
	if err != nil {
		t.Fatalf("Fetch of exactly %d-byte body: %v", MaxBodyBytes, err)
	}
	if int64(len(res.Body)) != MaxBodyBytes {
		t.Errorf("len(Body) = %d, want %d", len(res.Body), MaxBodyBytes)
	}
}

// TestFetchEmptyBody: a 200 with no usable payload is an empty_body error
// so the caller keeps its previous configs instead of parsing garbage.
func TestFetchEmptyBody(t *testing.T) {
	cases := map[string]string{
		"empty":         "",
		"whitespace":    "  \n\t \n",
		"empty base64":  "",
		"newlines only": "\n\n\n",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprint(w, body)
			}))
			defer srv.Close()

			res, err := Fetch(srv.URL, Options{AllowHTTP: true})
			if err == nil {
				t.Fatalf("Fetch of %q: expected error, got body %q", body, res.Body)
			}
			if got := KindOf(err); got != KindEmptyBody {
				t.Errorf("KindOf(err) = %q, want %q (err: %v)", got, KindEmptyBody, err)
			}
		})
	}
}

// TestFetchConditionalGetNotModified: validators from a previous response
// are replayed as If-None-Match / If-Modified-Since, and a 304 is a
// first-class result (nil error, NotModified set) — not a failure.
func TestFetchConditionalGetNotModified(t *testing.T) {
	const (
		etag = `"sub-v1"`
		lm   = "Wed, 07 Oct 2026 00:00:00 GMT"
	)
	body := "ss://YWVzLTEyOC1nY206cGFzc3dvcmQ=@1.2.3.4:12345#TestSS"

	var mu sync.Mutex
	var inm, ims string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		inm, ims = r.Header.Get("If-None-Match"), r.Header.Get("If-Modified-Since")
		mu.Unlock()
		if inm == etag && ims == lm {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", etag)
		w.Header().Set("Last-Modified", lm)
		fmt.Fprint(w, body)
	}))
	defer srv.Close()

	// First fetch: no validators, expect the body plus the response's
	// validators for the next round trip.
	first, err := Fetch(srv.URL, Options{AllowHTTP: true})
	if err != nil {
		t.Fatalf("first Fetch: %v", err)
	}
	if string(first.Body) != body {
		t.Fatalf("first Body = %q, want %q", first.Body, body)
	}
	if first.ETag != etag || first.LastModified != lm {
		t.Fatalf("first validators = ETag %q Last-Modified %q, want %q / %q",
			first.ETag, first.LastModified, etag, lm)
	}

	// Second fetch: replay the stored validators.
	second, err := Fetch(srv.URL, Options{
		AllowHTTP:    true,
		ETag:         first.ETag,
		LastModified: first.LastModified,
	})
	if err != nil {
		t.Fatalf("conditional Fetch: expected nil error for 304, got %v", err)
	}
	if !second.NotModified {
		t.Fatalf("NotModified = false, want true (body %q)", second.Body)
	}
	if len(second.Body) != 0 {
		t.Errorf("304 body = %q, want empty", second.Body)
	}
	if Outcome(second, nil) != KindNotModified {
		t.Errorf("Outcome = %q, want %q", Outcome(second, nil), KindNotModified)
	}

	mu.Lock()
	defer mu.Unlock()
	if inm != etag {
		t.Errorf("If-None-Match = %q, want %q", inm, etag)
	}
	if ims != lm {
		t.Errorf("If-Modified-Since = %q, want %q", ims, lm)
	}
}

// TestFetchNonSuccessStatus: a non-2xx response is a status error, distinct
// from transport / scheme / size failures.
func TestFetchNonSuccessStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	res, err := Fetch(srv.URL, Options{AllowHTTP: true})
	if err == nil {
		t.Fatalf("Fetch of 500: expected error, got body %q", res.Body)
	}
	if got := KindOf(err); got != KindStatus {
		t.Errorf("KindOf(err) = %q, want %q (err: %v)", got, KindStatus, err)
	}
}

// TestFetchTransportError: a connection failure is a transport error, not a
// scheme or status problem.
func TestFetchTransportError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := srv.URL
	srv.Close()

	res, err := Fetch(url, Options{AllowHTTP: true})
	if err == nil {
		t.Fatalf("Fetch against closed server: expected error, got body %q", res.Body)
	}
	if got := KindOf(err); got != KindTransport {
		t.Errorf("KindOf(err) = %q, want %q (err: %v)", got, KindTransport, err)
	}
	if Outcome(res, err) != KindTransport {
		t.Errorf("Outcome = %q, want %q", Outcome(res, err), KindTransport)
	}
}

// TestFetchReadIsBounded: even with a body far larger than the cap on
// offer, Fetch pulls at most MaxBodyBytes+1 bytes off the wire before
// reporting oversized — the read itself is bounded, not just the verdict.
func TestFetchReadIsBounded(t *testing.T) {
	offer := &budgetBody{budget: 64 << 20} // 64 MiB available: 8x the cap
	client := &http.Client{Transport: &budgetTransport{body: offer}}

	res, err := Fetch("https://subs.example/sub", Options{Client: client})
	if err == nil {
		t.Fatalf("expected oversized error, got %d bytes", len(res.Body))
	}
	if got := KindOf(err); got != KindOversized {
		t.Errorf("KindOf(err) = %q, want %q (err: %v)", got, KindOversized, err)
	}
	if offer.n > MaxBodyBytes+1 {
		t.Errorf("Fetch read %d bytes, want at most %d (cap + 1)", offer.n, MaxBodyBytes+1)
	}
}

// budgetBody hands out a fixed number of bytes and then EOFs, recording how
// many bytes the caller actually pulled.
type budgetBody struct {
	budget int64
	n      int64
}

func (b *budgetBody) Read(p []byte) (int, error) {
	if b.n >= b.budget {
		return 0, io.EOF
	}
	if remain := b.budget - b.n; int64(len(p)) > remain {
		p = p[:remain]
	}
	for i := range p {
		p[i] = 'a'
	}
	b.n += int64(len(p))
	return len(p), nil
}

func (b *budgetBody) Close() error { return nil }

// budgetTransport serves budgetBody without touching the network.
type budgetTransport struct {
	body *budgetBody
}

func (t *budgetTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode:    http.StatusOK,
		Status:        "200 OK",
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        make(http.Header),
		Body:          t.body,
		ContentLength: -1,
		Request:       req,
	}, nil
}

// TestFetchKindsAreDistinguishable: every documented outcome kind has a
// distinct value, so callers can branch on them.
func TestFetchKindsAreDistinguishable(t *testing.T) {
	kinds := []Kind{KindOK, KindTransport, KindBadScheme, KindOversized, KindStatus, KindEmptyBody, KindNotModified}
	seen := map[Kind]bool{}
	for _, k := range kinds {
		if k == "" {
			t.Errorf("empty kind in %v", kinds)
		}
		if seen[k] {
			t.Errorf("duplicate kind %q", k)
		}
		seen[k] = true
	}
}
