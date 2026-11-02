package products

import (
	"strings"
	"testing"
	"time"
)

const (
	testToken = "svc-token-SECRET-do-not-leak"
	pid       = "0190a1b2-0000-7000-8000-000000000001"
	aid       = "0190a1b2-0000-7000-8000-0000000000a1"
)

func TestNew_ValidatesItsOptionsWithoutEchoingSecrets(t *testing.T) {
	ok := Options{BaseURL: "http://synkro-products-api:8080", Token: testToken, Timeout: time.Second, Attempts: 1}
	tests := map[string]func(*Options){
		"empty token":        func(o *Options) { o.Token = "" },
		"token with newline": func(o *Options) { o.Token = "abc\r\nX-Evil: 1" },
		"token with space":   func(o *Options) { o.Token = "abc def" },
		"empty url":          func(o *Options) { o.BaseURL = "" },
		"no scheme":          func(o *Options) { o.BaseURL = "synkro-products-api:8080" },
		"ftp scheme":         func(o *Options) { o.BaseURL = "ftp://host" },
		"no host":            func(o *Options) { o.BaseURL = "http://" },
		"userinfo in url":    func(o *Options) { o.BaseURL = "http://user:s3cret@host:8080" },
		"query in url":       func(o *Options) { o.BaseURL = "http://host:8080/?x=1" },
		"zero timeout":       func(o *Options) { o.Timeout = 0 },
		"zero attempts":      func(o *Options) { o.Attempts = 0 },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			o := ok
			mutate(&o)

			_, err := New(o)

			if err == nil {
				t.Fatal("expected an error")
			}
			for _, secret := range []string{testToken, "s3cret", "X-Evil"} {
				if strings.Contains(err.Error(), secret) {
					t.Errorf("error leaks %q: %v", secret, err)
				}
			}
		})
	}
	if _, err := New(ok); err != nil {
		t.Errorf("valid options rejected: %v", err)
	}
}
