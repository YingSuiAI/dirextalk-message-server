# P2P Dual Full Regression Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the dual-node full P2P regression pass from a clean Docker volume rebuild.

**Architecture:** Keep ordinary messages on Matrix Client-Server APIs. P2P actions that change product membership must update durable P2P state and Matrix membership/policy state through `p2p.Transport`, so Matrix send/history/search sees the same rules as P2P lists.

**Tech Stack:** Go, Element Dendrite, Dirextalk P2P service, PostgreSQL 18, Docker Compose, PowerShell.

---

### Task 1: Make Dual-Node Search Validation Runnable

**Files:**
- Modify: `docker-compose.p2p-dual.yml`

- [ ] **Step 1: Reproduce the search blocker**

Run:

```powershell
$env:P2P_DUAL_PUBLIC_HOST='host.docker.internal'
docker compose -f docker-compose.p2p-dual.yml down -v --remove-orphans
docker compose -f docker-compose.p2p-dual.yml up -d --build --force-recreate dendrite-a dendrite-b
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\p2p-dual-smoke.ps1 -FederationWaitSeconds 90
```

Expected failure before the fix:

```text
Matrix search failed ... The remote server returned an error: (501) Not Implemented.
```

- [ ] **Step 2: Enable fulltext search in generated dual-node configs**

In both `dendrite-a-init` and `dendrite-b-init` command blocks, immediately after:

```sh
sed -i 's/disable_tls_validation: false/disable_tls_validation: true/' /etc/dirextalk-message-server/message-server.yaml
```

add:

```sh
sed -i '/^  search:/,/^user_api:/s/enabled: false/enabled: true/' /etc/dirextalk-message-server/message-server.yaml
```

- [ ] **Step 3: Validate compose config**

Run:

```powershell
$env:P2P_DUAL_PUBLIC_HOST='host.docker.internal'
docker compose -f docker-compose.p2p-dual.yml config --quiet
```

Expected: command exits 0.

### Task 2: Block Matrix Sends After Contact Deletion

**Files:**
- Modify: `p2p/service.go`
- Test: `p2p/transport_test.go`

- [ ] **Step 1: Write the failing transport test**

Add this test after `TestContactAcceptJoinsDirectRoomThroughTransport`:

```go
func TestContactDeleteLeavesDirectRoomThroughTransport(t *testing.T) {
	transport := &recordingTransport{}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:      "!dm:remote.example",
		PeerMXID:    "@alice:remote.example",
		DisplayName: "Alice",
		Domain:      "remote.example",
		Status:      "accepted",
	}); err != nil {
		t.Fatal(err)
	}

	result := mustHandle[map[string]any](t, service, "contacts.delete", map[string]any{
		"room_id": "!dm:remote.example",
	})

	if result["status"] != "ok" {
		t.Fatalf("expected delete status ok, got %#v", result)
	}
	if len(transport.leaves) != 1 || transport.leaves[0] != "@owner:example.com from !dm:remote.example" {
		t.Fatalf("expected contact delete to leave direct room through transport, got %#v", transport.leaves)
	}
}
```

- [ ] **Step 2: Run the failing test**

Run:

```powershell
go test ./p2p -run TestContactDeleteLeavesDirectRoomThroughTransport -count=1
```

Expected before implementation: FAIL because `contacts.delete` does not call `LeaveRoom`.

- [ ] **Step 3: Implement the minimal behavior**

In `contactMutation`, inside the `contacts.delete` / `contacts.requests.delete` branch after contact lookup and before setting `contact.Status = "deleted"`, call `s.transport.LeaveRoom` only for `contacts.delete`, only when the contact was not already deleted, and only when `contact.RoomID` is non-empty.

Use:

```go
wasDeleted := contactDeleted(contact.Status)
if action == "contacts.delete" && !wasDeleted && contact.RoomID != "" && s.transport != nil {
	s.mu.Lock()
	ownerMXID := s.ownerMXID
	s.mu.Unlock()
	if err := s.transport.LeaveRoom(ctx, LeaveRoomRequest{
		RoomID:   contact.RoomID,
		UserMXID: ownerMXID,
	}); err != nil {
		return nil, transportWriteError(err)
	}
}
```

- [ ] **Step 4: Run focused contact tests**

Run:

```powershell
gofmt -w p2p\service.go p2p\transport_test.go
go test ./p2p -run 'TestContact(DeleteLeavesDirectRoomThroughTransport|AcceptJoinsDirectRoomThroughTransport|RequestCreatesDirectInviteRoomThroughTransport|DeletedContactCannotMessageOrRequestAgain|DeletedContactStillBlocksMessageAndRequestAfterReload)' -count=1
```

Expected: PASS.

### Task 3: Full Dual-Node Acceptance

**Files:**
- Read: `scripts/p2p-dual-smoke.ps1`
- Read: `docs/p2p-dual-full-test-report.md`

- [ ] **Step 1: Rebuild clean dual-node environment**

Run:

```powershell
$env:P2P_DUAL_PUBLIC_HOST='host.docker.internal'
docker compose -f docker-compose.p2p-dual.yml down -v --remove-orphans
docker compose -f docker-compose.p2p-dual.yml up -d --build --force-recreate dendrite-a dendrite-b
```

Expected: A/B Dendrite and PostgreSQL containers become healthy.

- [ ] **Step 2: Run full dual smoke**

Run:

```powershell
$env:P2P_DUAL_PUBLIC_HOST='host.docker.internal'
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\p2p-dual-smoke.ps1 -FederationWaitSeconds 90
```

Expected: PASS with final JSON containing `"status": "ok"`.

- [ ] **Step 3: Run final static checks**

Run:

```powershell
git diff --check
```

Expected: command exits 0.

If Postman changes are made, also run:

```powershell
Get-Content docs/postman/dirextalk-message-server.postman_collection.json | ConvertFrom-Json | Out-Null
```
