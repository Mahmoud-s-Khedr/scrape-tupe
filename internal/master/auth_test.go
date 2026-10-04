package master

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/Mahmoud-s-Khedr/scrape-tupe/internal/db"
)

func authServer(t *testing.T) *Server {
	t.Helper()
	database, err := db.Open(filepath.Join(t.TempDir(), "auth.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return New(database, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestWorkerAuthentication(t *testing.T) {
	for _, tc := range []struct {
		name, token, email string
		validateErr        error
		want               int
	}{
		{"valid allowlisted", "good", "normalizer@example", nil, http.StatusBadRequest},
		{"valid unallowlisted", "other", "other@example", nil, http.StatusUnauthorized},
		{"malformed", "bad", "", errors.New("bad token"), http.StatusUnauthorized},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := authServer(t)
			s.SetAuth("", "https://master.example")
			s.SetWorkerServiceAccounts("normalizer@example")
			s.SetTokenValidator(func(_ context.Context, token, audience string) (string, error) {
				if audience != "https://master.example" {
					t.Fatalf("audience=%q", audience)
				}
				if token != tc.token {
					t.Fatalf("token=%q", token)
				}
				return tc.email, tc.validateErr
			})
			r := httptest.NewRequest(http.MethodPost, "/normalizer/urls/claim", nil)
			r.Header.Set("Authorization", "Bearer "+tc.token)
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, r)
			if w.Code != tc.want {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
		})
	}
}

func TestWorkerAuthenticationLocalOpenOnlyWhenUnset(t *testing.T) {
	s := authServer(t)
	r := httptest.NewRequest(http.MethodPost, "/normalizer/urls/claim", nil)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("local status=%d", w.Code)
	}
	s.SetAuth("", "https://master.example")
	if err := s.ValidateAuthConfig(); err == nil {
		t.Fatal("expected invalid production auth configuration")
	}
}
