package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	integrationsdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-integration-sdk/remote"
)

func TestStandaloneOAuthChild(t *testing.T) {
	if os.Getenv("DOMAINRY_OAUTH_PROCESS_TEST") != "1" {
		return
	}
	if err := run(); err != nil {
		t.Fatal(err)
	}
}

func TestStandaloneOAuthConfigurationAndSessionSurviveProcessRestart(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	baseURL := "http://" + address
	path := filepath.Join(t.TempDir(), "standalone.db")
	var child *exec.Cmd
	var done chan error
	var binding integrationsdk.Binding
	start := func() {
		t.Helper()
		child = exec.Command(os.Args[0], "-test.run=^TestStandaloneOAuthChild$")
		child.Env = append(os.Environ(), "DOMAINRY_OAUTH_PROCESS_TEST=1", "INTEGRATION_RUNTIME_ID=oauth-process", "INTEGRATION_SERVICE_TOKEN=isolated-service-token", "INTEGRATION_MASTER_KEY="+base64.StdEncoding.EncodeToString([]byte(strings.Repeat("k", 32))), "INTEGRATION_SQLITE_PATH="+path, "INTEGRATION_HTTP_ADDRESS="+address)
		child.Stdout, child.Stderr = os.Stdout, os.Stderr
		if err := child.Start(); err != nil {
			t.Fatal(err)
		}
		done = make(chan error, 1)
		go func(command *exec.Cmd) { done <- command.Wait() }(child)
		client := &http.Client{Timeout: time.Second}
		var descriptor integrationsdk.Descriptor
		deadline := time.Now().Add(10 * time.Second)
		for {
			request, _ := http.NewRequest("GET", baseURL+"/integration/v1/descriptor", nil)
			request.Header.Set("Authorization", "Bearer isolated-service-token")
			request.Header.Set("X-Domainry-Runtime-ID", "product")
			response, err := client.Do(request)
			if err == nil {
				err = json.NewDecoder(response.Body).Decode(&descriptor)
				response.Body.Close()
				if err == nil && response.StatusCode == 200 {
					break
				}
			}
			select {
			case err := <-done:
				t.Fatalf("child stopped: %v", err)
			default:
			}
			if time.Now().After(deadline) {
				t.Fatal("standalone process did not start")
			}
			time.Sleep(20 * time.Millisecond)
		}
		binding, err = remote.NewFactory(remote.Options{BaseURL: baseURL, Token: "isolated-service-token", HTTPClient: client}).OpenSaaS(t.Context(), integrationsdk.ApplicationRef{RuntimeID: "product"}, nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	stop := func() {
		t.Helper()
		if binding != nil {
			_ = binding.Close(context.Background())
			binding = nil
		}
		if child == nil {
			return
		}
		_ = child.Process.Signal(os.Interrupt)
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("child shutdown: %v", err)
			}
		case <-time.After(10 * time.Second):
			_ = child.Process.Kill()
			<-done
			t.Error("child shutdown timed out")
		}
		child = nil
	}
	t.Cleanup(stop)
	start()
	unauthorized, err := http.Get(baseURL + "/integration/v1/oauth-authorizations/options?workspace_id=w&user_id=u&allow_personal=true")
	if err != nil {
		t.Fatal(err)
	}
	unauthorized.Body.Close()
	if unauthorized.StatusCode != 401 {
		t.Fatal("service-token boundary bypassed")
	}
	admin := binding.(integrationsdk.OAuthApplicationsBinding).OAuthApplications()
	input := integrationsdk.OAuthApplicationInput{ConnectorKey: "google_workspace", ProviderKey: "google", Name: "Google", ClientID: "isolated-client", ClientSecret: "isolated-client-secret", RedirectURI: "http://127.0.0.1/oauth/callback", Scopes: []string{"https://www.googleapis.com/auth/calendar.readonly"}, Enabled: true}
	if _, err := admin.UpsertOAuthApplication(t.Context(), "w", "google", "admin", input); err != nil {
		t.Fatal(err)
	}
	input.ConnectorKey, input.ProviderKey, input.Name = "microsoft_365", "microsoft", "Microsoft"
	input.ConnectionConfig = map[string]any{"tenant_id": "common"}
	input.Scopes = []string{"offline_access", "User.Read"}
	if _, err := admin.UpsertOAuthApplication(t.Context(), "w", "microsoft", "admin", input); err != nil {
		t.Fatal(err)
	}
	subject := integrationsdk.ConnectionAccountSubject{WorkspaceID: "w", UserID: "u", Access: integrationsdk.ConnectionAccountAccess{Personal: true}}
	auth := binding.(integrationsdk.OAuthAuthorizationsBinding).OAuthAuthorizations()
	session, err := auth.StartOAuthAuthorization(t.Context(), subject, integrationsdk.OAuthAuthorizationInput{ApplicationKey: "google", Scope: integrationsdk.ConnectionAccountScopePersonal, Scopes: []string{"https://www.googleapis.com/auth/calendar.readonly"}})
	if err != nil {
		t.Fatal(err)
	}
	location, _ := url.Parse(session.AuthorizationURL)
	if location.Host != "accounts.google.com" || location.Query().Get("code_challenge_method") != "S256" {
		t.Fatal("official authorization URL missing PKCE")
	}
	state := location.Query().Get("state")
	stop()
	start()
	auth = binding.(integrationsdk.OAuthAuthorizationsBinding).OAuthAuthorizations()
	options, err := auth.ListOAuthAuthorizationOptions(t.Context(), subject)
	if err != nil || len(options) != 2 {
		t.Fatalf("configured applications did not survive: %v", err)
	}
	stored, err := auth.GetOAuthAuthorization(t.Context(), subject, session.ID)
	if err != nil || stored.Status != "pending" || stored.AuthorizationURL != "" {
		t.Fatalf("safe session restore: %v", err)
	}
	denied, err := auth.CompleteOAuthAuthorization(t.Context(), subject, integrationsdk.OAuthAuthorizationCallback{State: state, Error: "access_denied"})
	if err != nil || denied.Status != "rejected" {
		t.Fatalf("denial completion: %v", err)
	}
	stop()
	start()
	auth = binding.(integrationsdk.OAuthAuthorizationsBinding).OAuthAuthorizations()
	stored, err = auth.GetOAuthAuthorization(t.Context(), subject, session.ID)
	if err != nil || stored.Status != "rejected" {
		t.Fatalf("terminal receipt lost: %v", err)
	}
	stop()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{"isolated-client-secret", state} {
		if strings.Contains(string(data), private) {
			t.Fatal("private material persisted in plaintext")
		}
	}
}
