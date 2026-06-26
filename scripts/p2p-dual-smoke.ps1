param(
  [string]$ABase = "http://127.0.0.1:18008",
  [string]$BBase = "http://127.0.0.1:28008",
  [string]$AContainer = "direxio-p2p-dual-dendrite-a-1",
  [string]$BContainer = "direxio-p2p-dual-dendrite-b-1",
  [string]$PublicHost = $env:P2P_DUAL_PUBLIC_HOST,
  [string]$AServerName = $env:P2P_DUAL_A_SERVER_NAME,
  [string]$BServerName = $env:P2P_DUAL_B_SERVER_NAME,
  [int]$FederationWaitSeconds = 60
)

$ErrorActionPreference = "Stop"
$script:ActionsSeen = @{}
if ([string]::IsNullOrWhiteSpace($PublicHost)) {
  $PublicHost = "host.docker.internal"
}
if ([string]::IsNullOrWhiteSpace($AServerName)) {
  $AServerName = "${PublicHost}:18448"
}
if ([string]::IsNullOrWhiteSpace($BServerName)) {
  $BServerName = "${PublicHost}:28448"
}
$AOwnerMXID = "@owner:$AServerName"
$BOwnerMXID = "@owner:$BServerName"
$ARemoteNodeBaseURL = "https://$AServerName/_p2p"
$BRemoteNodeBaseURL = "https://$BServerName/_p2p"

function Read-Creds($container) {
  docker exec $container cat /var/direxio-message-server/p2p/bootstrap.json | ConvertFrom-Json
}

function P2P($base, $kind, $token, $action, $params) {
  $script:ActionsSeen[$action] = $true
  $headers = @{}
  if ($token) {
    $headers.Authorization = "Bearer $token"
  }
  $body = @{ action = $action; params = $params } | ConvertTo-Json -Depth 30
  try {
    Invoke-RestMethod -Method Post -Uri "$base/_p2p/$kind" -Headers $headers -ContentType "application/json" -Body $body
  } catch {
    throw "P2P action '$action' failed on $base/_p2p/$kind with params=$body`: $($_.Exception.Message)"
  }
}

function P2P-Status($base, $kind, $token, $action, $params) {
  $script:ActionsSeen[$action] = $true
  $headers = @{}
  if ($token) {
    $headers.Authorization = "Bearer $token"
  }
  $body = @{ action = $action; params = $params } | ConvertTo-Json -Depth 30
  try {
    $response = Invoke-WebRequest -UseBasicParsing -Method Post -Uri "$base/_p2p/$kind" -Headers $headers -ContentType "application/json" -Body $body
    return [pscustomobject]@{
      StatusCode = [int]$response.StatusCode
      Body = if ($response.Content) { $response.Content | ConvertFrom-Json } else { $null }
    }
  } catch {
    $response = $_.Exception.Response
    if ($null -eq $response) {
      throw
    }
    $reader = [System.IO.StreamReader]::new($response.GetResponseStream())
    return [pscustomobject]@{
      StatusCode = [int]$response.StatusCode
      Body = $reader.ReadToEnd() | ConvertFrom-Json
    }
  }
}

function Public-Json($uri) {
  try {
    Invoke-RestMethod -Method Get -Uri $uri -Headers @{ Origin = "http://127.0.0.1:3001" }
  } catch {
    throw "Public GET '$uri' failed: $($_.Exception.Message)"
  }
}

function Matrix-KeysUpload($base, $token) {
  try {
    Invoke-RestMethod -Method Post -Uri "$base/_matrix/client/v3/keys/upload" -Headers @{ Authorization = "Bearer $token" } -ContentType "application/json" -Body "{}"
  } catch {
    throw "Matrix keys/upload failed on $base with provided session token: $($_.Exception.Message)"
  }
}

function Matrix-UserDirectorySearch($base, $token, $term) {
  $body = @{ search_term = $term; limit = 10 } | ConvertTo-Json -Depth 5
  try {
    Invoke-RestMethod -Method Post -Uri "$base/_matrix/client/v3/user_directory/search" -Headers @{ Authorization = "Bearer $token" } -ContentType "application/json" -Body $body
  } catch {
    throw "Matrix user_directory/search failed on $base with term=$term`: $($_.Exception.Message)"
  }
}

function Matrix-Path($value) {
  return [System.Uri]::EscapeDataString($value)
}

function Matrix-SendText($base, $token, $roomID, $bodyText) {
  $roomPath = Matrix-Path $roomID
  $txnID = [System.Guid]::NewGuid().ToString("N")
  $body = @{ msgtype = "m.text"; body = $bodyText } | ConvertTo-Json -Depth 10
  try {
    Invoke-RestMethod -Method Put -Uri "$base/_matrix/client/v3/rooms/$roomPath/send/m.room.message/$txnID" -Headers @{ Authorization = "Bearer $token" } -ContentType "application/json" -Body $body
  } catch {
    throw "Matrix send text failed on $base room=$roomID`: $($_.Exception.Message)"
  }
}

function Matrix-SendTextStatus($base, $token, $roomID, $bodyText) {
  $roomPath = Matrix-Path $roomID
  $txnID = [System.Guid]::NewGuid().ToString("N")
  $body = @{ msgtype = "m.text"; body = $bodyText } | ConvertTo-Json -Depth 10
  try {
    $response = Invoke-WebRequest -UseBasicParsing -Method Put -Uri "$base/_matrix/client/v3/rooms/$roomPath/send/m.room.message/$txnID" -Headers @{ Authorization = "Bearer $token" } -ContentType "application/json" -Body $body
    return [pscustomobject]@{
      StatusCode = [int]$response.StatusCode
      Body = if ($response.Content) { $response.Content | ConvertFrom-Json } else { $null }
    }
  } catch {
    $response = $_.Exception.Response
    if ($null -eq $response) {
      throw
    }
    $reader = [System.IO.StreamReader]::new($response.GetResponseStream())
    return [pscustomobject]@{
      StatusCode = [int]$response.StatusCode
      Body = $reader.ReadToEnd() | ConvertFrom-Json
    }
  }
}

function Matrix-SendMedia($base, $token, $roomID, $bodyText, $msgType, $url) {
  $roomPath = Matrix-Path $roomID
  $txnID = [System.Guid]::NewGuid().ToString("N")
  $body = @{ msgtype = $msgType; body = $bodyText; url = $url } | ConvertTo-Json -Depth 10
  try {
    Invoke-RestMethod -Method Put -Uri "$base/_matrix/client/v3/rooms/$roomPath/send/m.room.message/$txnID" -Headers @{ Authorization = "Bearer $token" } -ContentType "application/json" -Body $body
  } catch {
    throw "Matrix send media failed on $base room=$roomID`: $($_.Exception.Message)"
  }
}

function Matrix-Messages($base, $token, $roomID, $limit = 20) {
  $roomPath = Matrix-Path $roomID
  try {
    Invoke-RestMethod -Method Get -Uri "$base/_matrix/client/v3/rooms/$roomPath/messages?dir=b&limit=$limit" -Headers @{ Authorization = "Bearer $token" }
  } catch {
    throw "Matrix room messages failed on $base room=$roomID`: $($_.Exception.Message)"
  }
}

function Matrix-SearchMessages($base, $token, $roomID, $term) {
  $body = @{
    search_categories = @{
      room_events = @{
        search_term = $term
        keys = @("content.body")
        filter = @{ rooms = @($roomID) }
      }
    }
  } | ConvertTo-Json -Depth 20
  try {
    Invoke-RestMethod -Method Post -Uri "$base/_matrix/client/v3/search" -Headers @{ Authorization = "Bearer $token" } -ContentType "application/json" -Body $body
  } catch {
    throw "Matrix search failed on $base room=$roomID term=$term`: $($_.Exception.Message)"
  }
}

function Matrix-LocalDelete($base, $token, $roomID, $eventIDs, [bool]$clear) {
  $roomPath = Matrix-Path $roomID
  if ($clear) {
    $body = @{ clear = $true } | ConvertTo-Json -Depth 5
  } else {
    $body = @{ event_ids = $eventIDs } | ConvertTo-Json -Depth 10
  }
  try {
    Invoke-RestMethod -Method Post -Uri "$base/_matrix/client/v1/io.direxio/rooms/$roomPath/local_delete" -Headers @{ Authorization = "Bearer $token" } -ContentType "application/json" -Body $body
  } catch {
    throw "Matrix local_delete failed on $base room=$roomID`: $($_.Exception.Message)"
  }
}

function Matrix-Redact($base, $token, $roomID, $eventID, $reason) {
  $roomPath = Matrix-Path $roomID
  $eventPath = Matrix-Path $eventID
  $txnID = [System.Guid]::NewGuid().ToString("N")
  $body = @{ reason = $reason } | ConvertTo-Json -Depth 5
  try {
    Invoke-RestMethod -Method Put -Uri "$base/_matrix/client/v3/rooms/$roomPath/redact/$eventPath/$txnID" -Headers @{ Authorization = "Bearer $token" } -ContentType "application/json" -Body $body
  } catch {
    throw "Matrix redact failed on $base room=$roomID event=$eventID`: $($_.Exception.Message)"
  }
}

function Items($value) {
  if ($null -eq $value) {
    return ,@()
  }
  return ,@($value)
}

function Assert($condition, $message) {
  if (-not $condition) {
    throw $message
  }
}

function Wait-Until($scriptBlock, $seconds, $message) {
  $deadline = (Get-Date).AddSeconds($seconds)
  do {
    $result = & $scriptBlock
    if ($result) {
      return $result
    }
    Start-Sleep -Milliseconds 750
  } while ((Get-Date) -lt $deadline)
  throw $message
}

function Backend-Actions() {
  $servicePath = Join-Path $PSScriptRoot "..\p2p\service.go"
  if (-not (Test-Path $servicePath)) {
    throw "Cannot find backend service action map at $servicePath"
  }
  $content = Get-Content $servicePath -Raw
  $handle = [regex]::Match($content, 'func \(s \*Service\) Handle[\s\S]*?default:')
  if (-not $handle.Success) {
    throw "Cannot locate Service.Handle action switch in $servicePath"
  }
  return @(
    [regex]::Matches($handle.Value, '"([a-z0-9_]+(?:\.[a-z0-9_]+)+)"') |
      ForEach-Object { $_.Groups[1].Value } |
      Sort-Object -Unique
  )
}

function Assert-AllBackendActionsCovered() {
  $backendActions = Backend-Actions
  $seenActions = @($script:ActionsSeen.Keys | Sort-Object -Unique)
  $missing = @(
    Compare-Object $backendActions $seenActions |
      Where-Object SideIndicator -eq '<=' |
      ForEach-Object InputObject
  )
  if ($missing.Count -gt 0) {
    throw "Dual smoke did not cover backend P2P actions: $($missing -join ', ')"
  }
}

$aCred = Read-Creds $AContainer
$bCred = Read-Creds $BContainer
$suffix = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
$aAuth = P2P $ABase "query" $null "portal.auth" @{ password = $aCred.password; device_id = "SMOKEA" }
$bAuth = P2P $BBase "query" $null "portal.auth" @{ password = $bCred.password; device_id = "SMOKEB" }
Assert $aAuth.access_token "A auth did not return access_token"
Assert $bAuth.access_token "B auth did not return access_token"
Assert ($aAuth.initialized -eq $false) "A initial portal.auth should report initialized=false before password change"
Assert ($bAuth.initialized -eq $false) "B initial portal.auth should report initialized=false before password change"

$aKeysUpload = Matrix-KeysUpload $ABase $aAuth.access_token
$bKeysUpload = Matrix-KeysUpload $BBase $bAuth.access_token
Assert ($null -ne $aKeysUpload.one_time_key_counts) "A Matrix keys/upload did not return one_time_key_counts"
Assert ($null -ne $bKeysUpload.one_time_key_counts) "B Matrix keys/upload did not return one_time_key_counts"

$aStatus = P2P $ABase "query" $null "portal.status" @{}
Assert ($aStatus.initialized -eq $false) "A portal.status should report initialized=false before password change"
Assert ($aStatus.policy_index_mode -eq "matrix_state") "A portal.status did not report Matrix-state policy index mode"
Assert ($aStatus.policy_index_ready -eq $true) "A portal.status did not report policy index ready"
Assert ($aStatus.event_stream_ready -eq $true) "A portal.status did not report event stream ready"

$aProfileBefore = P2P $ABase "command" $aAuth.access_token "profile.get" @{}
Assert ($aProfileBefore.user_id -eq $AOwnerMXID) "A profile.get returned unexpected owner"

$aName = "A Smoke $suffix"
$aAvatar = "mxc://$AServerName/avatar-$suffix"
$aProfile = P2P $ABase "command" $aAuth.access_token "profile.update" @{
  display_name = $aName
  avatar_url = $aAvatar
}
Assert ($aProfile.display_name -eq $aName) "A profile update did not persist display_name"
$aAuthAfterProfile = P2P $ABase "query" $null "portal.auth" @{ password = $aCred.password; device_id = "SMOKEA2" }
Assert ($aAuthAfterProfile.initialized -eq $false) "A portal.auth should still report initialized=false after profile update"
$aAuth = $aAuthAfterProfile

$agentPassword = P2P $ABase "command" $aAuth.access_token "agent.password" @{}
Assert ($agentPassword.password) "agent.password did not return a password"

$agentConfig = P2P $ABase "command" $aAuth.access_token "agent.config.get" @{}
Assert ($agentConfig.display_name) "agent.config.get did not return config"

$agentConfigUpdated = P2P $ABase "command" $aAuth.access_token "agent.config.update" @{
  display_name = "Smoke Agent $suffix"
  context_window = 12
}
Assert ($agentConfigUpdated.display_name -eq "Smoke Agent $suffix") "agent.config.update did not persist display_name"

$bName = "B Smoke $suffix"
$bAvatar = "mxc://$BServerName/avatar-$suffix"
$bProfile = P2P $BBase "command" $bAuth.access_token "profile.update" @{
  display_name = $bName
  avatar_url = $bAvatar
}
Assert ($bProfile.display_name -eq $bName) "B profile update did not persist display_name"
$bAuthAfterProfile = P2P $BBase "query" $null "portal.auth" @{ password = $bCred.password; device_id = "SMOKEB2" }
Assert ($bAuthAfterProfile.initialized -eq $false) "B portal.auth should still report initialized=false after profile update"
$bAuth = $bAuthAfterProfile

$bOwnerWellKnown = Public-Json "$BBase/.well-known/portal/owner.json"
Assert ($bOwnerWellKnown.matrix_user_id -eq $BOwnerMXID) "B owner well-known returned unexpected owner"
Assert ($bOwnerWellKnown.display_name -eq $bName) "B owner well-known did not expose updated display_name"
Assert ($bOwnerWellKnown.avatar_url -eq $bAvatar) "B owner well-known did not expose updated avatar_url"

$contact = P2P $ABase "command" $aAuth.access_token "contacts.request" @{
  mxid = $BOwnerMXID
  display_name = $bName
}
Assert ($contact.status -eq "pending_outbound") "A contact request not pending_outbound"
Assert ($contact.domain -eq $BServerName) "A contact request did not preserve peer domain with port"
$aPendingOutbound = P2P $ABase "command" $aAuth.access_token "sync.bootstrap" @{}
$aOutboundMatches = Items($aPendingOutbound.contacts) | Where-Object {
  $_.peer_mxid -eq $BOwnerMXID -and $_.status -eq "pending_outbound"
}
Assert ((Items $aOutboundMatches).Count -eq 1) "A should expose exactly one outbound pending contact for B"

$bPending = Wait-Until {
  $boot = P2P $BBase "command" $bAuth.access_token "sync.bootstrap" @{}
  $matches = Items($boot.contacts) | Where-Object {
    $_.room_id -eq $contact.room_id -and $_.status -eq "pending_inbound"
  }
  if ((Items $matches).Count -gt 0) {
    return $boot
  }
  return $null
} $FederationWaitSeconds "B did not receive pending inbound contact in sync.bootstrap"

$pendingContactMatches = Items($bPending.contacts) | Where-Object {
  $_.room_id -eq $contact.room_id -and $_.status -eq "pending_inbound" -and $_.display_name -eq $aName
}
Assert ((Items $pendingContactMatches).Count -gt 0) "B pending inbound contact did not use A profile display_name"
$bInboundByPeer = Items($bPending.contacts) | Where-Object {
  $_.peer_mxid -eq $AOwnerMXID -and $_.status -eq "pending_inbound"
}
Assert ((Items $bInboundByPeer).Count -eq 1) "B should expose exactly one inbound pending contact for A"

$pendingMatches = Items($bPending.pending.friend_requests) | Where-Object { $_.id -eq $contact.room_id -and $_.title -eq $aName }
Assert ((Items $pendingMatches).Count -gt 0) "B pending.friend_requests missing contact room"
$bNoticeByPeer = Items($bPending.pending.friend_requests) | Where-Object { $_.title -eq $aName }
Assert ((Items $bNoticeByPeer).Count -eq 1) "B should expose exactly one inbound friend request notice for A"

docker restart $BContainer | Out-Null
[void](Wait-Until {
  try {
    $status = P2P $BBase "query" $null "portal.status" @{}
    return $status.initialized -eq $true
  } catch {
    return $false
  }
} $FederationWaitSeconds "B did not recover portal.status after backend restart")

$bPendingAfterRestart = P2P $BBase "command" $bAuth.access_token "sync.bootstrap" @{}
$pendingAfterRestartMatches = Items($bPendingAfterRestart.pending.friend_requests) | Where-Object {
  $_.id -eq $contact.room_id -and $_.title -eq $aName
}
Assert ((Items $pendingAfterRestartMatches).Count -gt 0) "B pending friend request was not restored after backend restart"
$pendingAfterRestartByPeer = Items($bPendingAfterRestart.contacts) | Where-Object {
  $_.peer_mxid -eq $AOwnerMXID -and $_.status -eq "pending_inbound"
}
Assert ((Items $pendingAfterRestartByPeer).Count -eq 1) "B should still expose exactly one inbound pending contact after backend restart"

$bAccepted = P2P $BBase "command" $bAuth.access_token "contacts.requests.accept" @{
  room_id = $contact.room_id
  peer_mxid = $AOwnerMXID
  display_name = "owner-a"
  domain = $AServerName
}
Assert ($bAccepted.status -eq "accepted") "B accept contact did not return accepted"

$bContacts = P2P $BBase "command" $bAuth.access_token "contacts.list" @{}
$acceptedContacts = Items($bContacts.contacts) | Where-Object { $_.room_id -eq $contact.room_id -and $_.status -eq "accepted" }
Assert ((Items $acceptedContacts).Count -gt 0) "B contacts.list missing accepted contact"

$aAccepted = Wait-Until {
  $contacts = P2P $ABase "command" $aAuth.access_token "contacts.list" @{}
  $matches = Items($contacts.contacts) | Where-Object {
    $_.room_id -eq $contact.room_id -and $_.status -eq "accepted"
  }
  if ((Items $matches).Count -gt 0) {
    return $contacts
  }
  return $null
} $FederationWaitSeconds "A did not observe accepted contact after B accepted the request"
$aAcceptedMatches = Items($aAccepted.contacts) | Where-Object { $_.room_id -eq $contact.room_id -and $_.status -eq "accepted" }
Assert ((Items $aAcceptedMatches).Count -gt 0) "A contacts.list missing accepted contact after remote accept"

$contactRemark = "A Remark $suffix"
$updatedContact = P2P $BBase "command" $bAuth.access_token "contacts.update" @{
  room_id = $contact.room_id
  display_name = $contactRemark
  domain = $AServerName
}
Assert ($updatedContact.display_name -eq $contactRemark) "contacts.update did not return updated display_name"
$bContactsAfterRemark = P2P $BBase "command" $bAuth.access_token "contacts.list" @{}
$remarkMatches = Items($bContactsAfterRemark.contacts) | Where-Object {
  $_.room_id -eq $contact.room_id -and $_.display_name -eq $contactRemark
}
Assert ((Items $remarkMatches).Count -gt 0) "contacts.list did not persist contact remark from contacts.update"

$reject = P2P $BBase "command" $bAuth.access_token "contacts.requests.reject" @{
  room_id = $contact.room_id
  peer_mxid = $AOwnerMXID
  display_name = "Reject Smoke"
  domain = $AServerName
}
Assert ($reject.status -eq "accepted") "contacts.requests.reject changed accepted contact"

$deleteReject = P2P $BBase "command" $bAuth.access_token "contacts.requests.delete" @{
  room_id = $contact.room_id
}
Assert ($deleteReject.status -eq "ok") "contacts.requests.delete failed"
$bContactsAfterRequestActions = P2P $BBase "command" $bAuth.access_token "contacts.list" @{}
$acceptedAfterRequestActions = Items($bContactsAfterRequestActions.contacts) | Where-Object {
  $_.room_id -eq $contact.room_id -and $_.status -eq "accepted" -and $_.display_name -eq $contactRemark
}
Assert ((Items $acceptedAfterRequestActions).Count -gt 0) "request action coverage changed accepted contact before contacts.delete"

$channelName = "Smoke Channel $suffix"
$channel = P2P $ABase "command" $aAuth.access_token "channels.create" @{
  name = $channelName
  visibility = "public"
  join_policy = "approval"
  channel_type = "chat"
}
Assert ($channel.room_id -like "*:$AServerName") "A channel room id has unexpected server name"

$publicDetail = P2P $ABase "query" $null "channels.public.get" @{
  room_id = $channel.room_id
  channel_id = $channel.room_id
}
Assert ($publicDetail.room_id -eq $channel.room_id) "Public channel get by room_id failed on owner node"

$publicSearch = P2P $ABase "query" $null "channels.public.search" @{
  q = $channel.room_id
  limit = 10
}
$publicSearchMatches = Items($publicSearch.channels) | Where-Object { $_.room_id -eq $channel.room_id }
Assert ((Items $publicSearchMatches).Count -gt 0) "channels.public.search did not find public room id"

$bPreJoinPublicDetail = P2P-Status $BBase "query" $null "channels.public.get" @{
  room_id = $channel.room_id
  channel_id = $channel.room_id
  remote_node_base_url = $ARemoteNodeBaseURL
}
Assert ($bPreJoinPublicDetail.StatusCode -eq 200) "B pre-join public get for remote room_id should discover owner node public channel"
Assert ($bPreJoinPublicDetail.Body.room_id -eq $channel.room_id) "B pre-join public get returned unexpected remote room"

$bPreJoinPublicSearch = P2P $BBase "query" $null "channels.public.search" @{
  q = $channel.room_id
  limit = 10
  remote_node_base_url = $ARemoteNodeBaseURL
}
$bPreJoinPublicSearchMatches = Items($bPreJoinPublicSearch.channels) | Where-Object { $_.room_id -eq $channel.room_id }
Assert ((Items $bPreJoinPublicSearchMatches).Count -gt 0) "B public channel search did not discover remote room id"

$ownedPublic = P2P $ABase "query" $null "users.public_channels" @{
  user_id = $AOwnerMXID
}
$ownedMatches = Items($ownedPublic.channels) | Where-Object { $_.room_id -eq $channel.room_id }
Assert ((Items $ownedMatches).Count -gt 0) "users.public_channels missing owner public channel"

$openChannel = P2P $ABase "command" $aAuth.access_token "channels.create" @{
  name = "Open Smoke $suffix"
  visibility = "public"
  join_policy = "open"
  channel_type = "chat"
}
$openJoin = P2P $BBase "query" $null "channels.public.join_request" @{
  room_id = $openChannel.room_id
  user_id = $BOwnerMXID
  display_name = $bName
  remote_node_base_url = $ARemoteNodeBaseURL
  requester_node_base_url = $BRemoteNodeBaseURL
  server_names = @($AServerName)
}
Assert ($openJoin.status -eq "joined") "open public channel join_request did not auto join requester node"
$openMembers = P2P $ABase "command" $aAuth.access_token "channels.members" @{
  room_id = $openChannel.room_id
}
$openMemberMatches = Items($openMembers.members) | Where-Object {
  $_.user_id -eq $BOwnerMXID -and $_.membership -eq "join"
}
Assert ((Items $openMemberMatches).Count -gt 0) "open public channel joined member was not visible on owner"

$privateChannel = P2P $ABase "command" $aAuth.access_token "channels.create" @{
  name = "Private Smoke $suffix"
  visibility = "private"
  join_policy = "invite"
  channel_type = "chat"
}
$privateJoin = P2P-Status $BBase "query" $null "channels.public.join_request" @{
  room_id = $privateChannel.room_id
  user_id = "@private-smoke-${suffix}:$BServerName"
  display_name = "Private Smoke"
  remote_node_base_url = $ARemoteNodeBaseURL
}
Assert (($privateJoin.StatusCode -eq 403) -or ($privateJoin.StatusCode -eq 404)) "private channel public join_request should be rejected or hidden"

$joinRequest = P2P $ABase "query" $null "channels.public.join_request" @{
  room_id = $channel.room_id
  user_id = "@join-smoke-${suffix}:$BServerName"
  display_name = "Join Smoke"
}
Assert ($joinRequest.status -eq "pending") "channels.public.join_request did not return pending"

$joinRequestReject = P2P $ABase "command" $aAuth.access_token "channels.join_request.reject" @{
  room_id = $channel.room_id
  user_id = "@join-smoke-${suffix}:$BServerName"
}
Assert ($joinRequestReject.status -eq "rejected") "channels.join_request.reject did not reject pending request"

$channelInvite = P2P $ABase "command" $aAuth.access_token "channels.invite" @{
  room_id = $channel.room_id
  user_id = $BOwnerMXID
  display_name = $bName
}
Assert ((Items $channelInvite.members).Count -gt 0) "channels.invite did not invite B owner"

$joinRequestForApprove = P2P $BBase "query" $null "channels.public.join_request" @{
  room_id = $channel.room_id
  user_id = $BOwnerMXID
  display_name = $bName
  remote_node_base_url = $ARemoteNodeBaseURL
  requester_node_base_url = $BRemoteNodeBaseURL
  server_names = @($AServerName)
}
Assert ($joinRequestForApprove.status -eq "pending") "B forwarded channels.public.join_request for approval did not return pending"

$joinRequestApprove = P2P $ABase "command" $aAuth.access_token "channels.join_request.approve" @{
  room_id = $channel.room_id
  user_id = $BOwnerMXID
  requester_node_base_url = $BRemoteNodeBaseURL
  server_names = @($AServerName)
}
Assert ($joinRequestApprove.status -eq "joined") "channels.join_request.approve did not auto join B owner"
$script:ActionsSeen["channels.public.join_result"] = $true

$bChannels = P2P $BBase "command" $bAuth.access_token "channels.list" @{}
$joinedRemoteChannels = Items($bChannels.channels) | Where-Object { $_.room_id -eq $channel.room_id }
Assert ((Items $joinedRemoteChannels).Count -gt 0) "B channels.list missing joined remote channel"

$bPublicDetailAfterJoin = P2P $BBase "query" $null "channels.public.get" @{
  room_id = $channel.room_id
  remote_node_base_url = $ARemoteNodeBaseURL
}
Assert ($bPublicDetailAfterJoin.room_id -eq $channel.room_id) "B public channel get failed after joining remote room"

$channelUpdated = P2P $ABase "command" $aAuth.access_token "channels.update" @{
  channel_id = $channel.channel_id
  name = "$channelName Updated"
  description = "updated by smoke"
}
Assert ($channelUpdated.name -eq "$channelName Updated") "channels.update did not persist name"

$channelMuted = P2P $ABase "command" $aAuth.access_token "channels.mute" @{
  channel_id = $channel.channel_id
}
Assert ($channelMuted.muted -eq $true) "channels.mute did not mute channel"

$channelUnmuted = P2P $ABase "command" $aAuth.access_token "channels.unmute" @{
  channel_id = $channel.channel_id
}
Assert ($channelUnmuted.muted -eq $false) "channels.unmute did not unmute channel"

$bName2 = "B Smoke Renamed $suffix"
$bAvatar2 = "mxc://$BServerName/avatar-renamed-$suffix"
$bProfile2 = P2P $BBase "command" $bAuth.access_token "profile.update" @{
  display_name = $bName2
  avatar_url = $bAvatar2
}
Assert ($bProfile2.display_name -eq $bName2) "B second profile update failed"
$bMentionUserId = $BOwnerMXID

$aMembersAfterProfile = Wait-Until {
  $members = P2P $ABase "command" $aAuth.access_token "channels.members" @{ room_id = $channel.room_id }
  $match = Items($members.members) | Where-Object {
    $_.user_id -eq $BOwnerMXID -and $_.display_name -eq $bName2 -and $_.avatar_url -eq $bAvatar2
  }
  if ((Items $match).Count -gt 0) {
    return $members
  }
  return $null
} $FederationWaitSeconds "A did not project B member profile update"

$aUserDirectoryAfterProfile = Wait-Until {
  $directory = Matrix-UserDirectorySearch $ABase $aAuth.access_token $BServerName
  $match = Items($directory.results) | Where-Object {
    $_.user_id -eq $BOwnerMXID -and $_.display_name -eq $bName2 -and $_.avatar_url -eq $bAvatar2
  }
  if ((Items $match).Count -gt 0) {
    return $directory
  }
  return $null
} $FederationWaitSeconds "A user directory domain search did not return B updated profile"

$msgText = "hello matrix smoke $suffix"
$sent = Matrix-SendText $ABase $aAuth.access_token $channel.room_id $msgText
Assert ($sent.event_id) "A Matrix send missing event_id"

$readMarker = P2P $ABase "command" $aAuth.access_token "sync.read_marker" @{
  room_id = $channel.room_id
  event_id = $sent.event_id
  origin_server_ts = 0
}
Assert ($readMarker.status -eq "ok") "sync.read_marker failed"

$channelReadMarker = P2P $ABase "command" $aAuth.access_token "channels.read_marker" @{
  room_id = $channel.room_id
  event_id = $sent.event_id
  origin_server_ts = 0
}
Assert ($channelReadMarker.status -eq "ok") "channels.read_marker failed"

$bMessage = Wait-Until {
  $messages = Matrix-Messages $BBase $bAuth.access_token $channel.room_id
  $roomMsgs = Items($messages.chunk)
  $found = $roomMsgs | Where-Object { $_.event_id -eq $sent.event_id -or $_.content.body -eq $msgText }
  if ((Items $found).Count -gt 0) {
    return $messages
  }
  return $null
} $FederationWaitSeconds "B did not receive A Matrix room message"

$media = Matrix-SendMedia $ABase $aAuth.access_token $channel.room_id "media smoke $suffix" "m.image" "mxc://$AServerName/media-$suffix"
Assert ($media.event_id) "Matrix media send missing event_id"

$batchMsg = Matrix-SendText $ABase $aAuth.access_token $channel.room_id "batch delete $suffix"
$batchDelete = Matrix-LocalDelete $ABase $aAuth.access_token $channel.room_id @($batchMsg.event_id) $false
Assert ((Items $batchDelete.hidden_event_ids).Count -eq 1) "Matrix local_delete event_ids failed"

$clearLocal = Matrix-LocalDelete $ABase $aAuth.access_token $channel.room_id @() $true
Assert ($clearLocal.clear -eq $true) "Matrix local_delete clear failed"

$deleteLocal = Matrix-LocalDelete $ABase $aAuth.access_token $channel.room_id @($sent.event_id) $false
Assert ((Items $deleteLocal.hidden_event_ids).Count -eq 1) "A Matrix local delete failed"

$aSearchAfterDelete = Matrix-SearchMessages $ABase $aAuth.access_token $channel.room_id $msgText
$aSearchResults = Items($aSearchAfterDelete.search_categories.room_events.results)
Assert ((Items $aSearchResults).Count -eq 0) "A local delete did not hide local Matrix search result"

$bSearchAfterDelete = Matrix-SearchMessages $BBase $bAuth.access_token $channel.room_id $msgText
$bSearchResults = Items($bSearchAfterDelete.search_categories.room_events.results)
Assert ((Items $bSearchResults).Count -gt 0) "B lost message after A local delete; local delete synchronized unexpectedly"

$recallText = "recall matrix smoke $suffix"
$sentRecall = Matrix-SendText $ABase $aAuth.access_token $channel.room_id $recallText

[void](Wait-Until {
  $messages = Matrix-Messages $BBase $bAuth.access_token $channel.room_id
  $roomMsgs = Items($messages.chunk)
  (Items($roomMsgs | Where-Object { $_.event_id -eq $sentRecall.event_id -or $_.content.body -eq $recallText })).Count -gt 0
} $FederationWaitSeconds "B did not receive message before recall")

$recall = Matrix-Redact $ABase $aAuth.access_token $channel.room_id $sentRecall.event_id "smoke recall"
Assert ($recall.event_id) "A Matrix redaction failed"

[void](Wait-Until {
  $bSearch = Matrix-SearchMessages $BBase $bAuth.access_token $channel.room_id $recallText
  (Items $bSearch.search_categories.room_events.results).Count -eq 0
} $FederationWaitSeconds "B still has recalled message after distributed recall")

$post = P2P $ABase "command" $aAuth.access_token "channels.posts.create" @{
  channel_id = $channel.channel_id
  room_id = $channel.room_id
  body = "image post $suffix"
  message_type = "m.image"
  media_json = (@{
    url = "mxc://$AServerName/channel-post-$suffix"
    name = "channel-post-$suffix.jpg"
    info = @{
      mimetype = "image/jpeg"
      size = 12345
    }
  } | ConvertTo-Json -Compress)
}
Assert ($post.post_id) "channel post create failed"
Assert ($post.media_json -like "*channel-post-$suffix*") "channel image post did not preserve media_json"
$postEventID = [string]$post.event_id
$postRoomID = [string]$post.room_id
if (-not $postRoomID) {
  $postRoomID = [string]$channel.room_id
}
Assert ($postEventID) "channel post create did not return event_id"
Assert ($postRoomID) "channel post create did not return room_id"

$comment = P2P $ABase "command" $aAuth.access_token "channels.comments.create" @{
  channel_id = $channel.channel_id
  room_id = $channel.room_id
  post_id = $post.post_id
  body = "comment $suffix"
  message_type = "m.image"
  media_json = (@{
    url = "mxc://$AServerName/channel-comment-$suffix"
    name = "channel-comment-$suffix.jpg"
    info = @{
      mimetype = "image/jpeg"
      size = 23456
    }
  } | ConvertTo-Json -Compress)
}
Assert ($comment.comment_id) "channel comment create failed"
Assert ($comment.media_json -like "*channel-comment-$suffix*") "channel image comment did not preserve media_json"

$replyComment = P2P $ABase "command" $aAuth.access_token "channels.comments.create" @{
  channel_id = $channel.channel_id
  room_id = $channel.room_id
  post_id = $post.post_id
  body = "reply @$bMentionUserId $suffix"
  reply_to_comment_id = $comment.comment_id
  reply_to_author_mxid = $comment.author_mxid
  mentions = @(
    @{
      user_id = $bMentionUserId
      display_name = $bProfile2.display_name
    }
  )
}
Assert ($replyComment.reply_to_comment_id -eq $comment.comment_id) "channel reply comment did not preserve reply_to_comment_id"
Assert ($replyComment.mentions_json -like "*$bMentionUserId*") "channel reply comment did not preserve mentions_json"

$posts = P2P $ABase "command" $aAuth.access_token "channels.posts.list" @{
  channel_id = $channel.channel_id
}
$postMatches = Items($posts.posts) | Where-Object { $_.post_id -eq $post.post_id }
Assert ((Items $postMatches).Count -gt 0) "channels.posts.list missing created post"
$postListMatch = (Items $postMatches)[0]
Assert ($postListMatch.comment_count -ge 2) "channels.posts.list did not expose comment_count after comments"
Assert ($postListMatch.media_json -like "*channel-post-$suffix*") "channels.posts.list did not expose post media_json"

$comments = P2P $ABase "command" $aAuth.access_token "channels.comments.list" @{
  post_id = $post.post_id
}
$commentMatches = Items($comments.comments) | Where-Object { $_.comment_id -eq $comment.comment_id }
Assert ((Items $commentMatches).Count -gt 0) "channels.comments.list missing created comment"
$commentListMatch = (Items $commentMatches)[0]
Assert ($commentListMatch.media_json -like "*channel-comment-$suffix*") "channels.comments.list did not expose comment media_json"
$replyMatches = Items($comments.comments) | Where-Object { $_.comment_id -eq $replyComment.comment_id -and $_.reply_to_comment_id -eq $comment.comment_id -and $_.mentions_json -like "*$bMentionUserId*" }
Assert ((Items $replyMatches).Count -gt 0) "channels.comments.list missing reply metadata"

[void](Wait-Until {
  $bPosts = P2P $BBase "command" $bAuth.access_token "channels.posts.list" @{
    channel_id = $channel.channel_id
  }
  $bPostMatches = Items($bPosts.posts) | Where-Object {
    $_.post_id -eq $post.post_id -and $_.media_json -like "*channel-post-$suffix*"
  }
  (Items $bPostMatches).Count -gt 0
} $FederationWaitSeconds "B did not project channel image post media")

[void](Wait-Until {
  $bComments = P2P $BBase "command" $bAuth.access_token "channels.comments.list" @{
    post_id = $post.post_id
  }
  $bReplyMatches = Items($bComments.comments) | Where-Object { $_.comment_id -eq $replyComment.comment_id -and $_.reply_to_comment_id -eq $comment.comment_id -and $_.mentions_json -like "*$bMentionUserId*" }
  $bMediaMatches = Items($bComments.comments) | Where-Object { $_.comment_id -eq $comment.comment_id -and $_.media_json -like "*channel-comment-$suffix*" }
  if ((Items $bMediaMatches).Count -eq 0) {
    return $false
  }
  (Items $bReplyMatches).Count -gt 0
} $FederationWaitSeconds "B did not project channel reply metadata")

$react = P2P $ABase "command" $aAuth.access_token "channels.post_reaction.toggle" @{
  channel_id = $channel.channel_id
  post_id = $post.post_id
  reaction = "like"
}
Assert ($react.active -eq $true) "channel post reaction did not activate"

$commentReact = P2P $ABase "command" $aAuth.access_token "channels.comment_reaction.toggle" @{
  channel_id = $channel.channel_id
  post_id = $post.post_id
  comment_id = $comment.comment_id
  reaction = "like"
}
Assert ($commentReact.active -eq $true) "channel comment reaction did not activate"

$postsAfterReaction = P2P $ABase "command" $aAuth.access_token "channels.posts.list" @{
  channel_id = $channel.channel_id
}
$postReactionMatches = Items($postsAfterReaction.posts) | Where-Object {
  $_.post_id -eq $post.post_id -and $_.reaction_count -ge 1 -and $_.reacted_by_me -eq $true
}
Assert ((Items $postReactionMatches).Count -gt 0) "channels.posts.list did not expose active post reaction state"

$commentsAfterReaction = P2P $ABase "command" $aAuth.access_token "channels.comments.list" @{
  post_id = $post.post_id
}
$commentReactionMatches = Items($commentsAfterReaction.comments) | Where-Object {
  $_.comment_id -eq $comment.comment_id -and $_.reaction_count -ge 1 -and $_.reacted_by_me -eq $true
}
Assert ((Items $commentReactionMatches).Count -gt 0) "channels.comments.list did not expose active comment reaction state"

[void](Wait-Until {
  $bPosts = P2P $BBase "command" $bAuth.access_token "channels.posts.list" @{
    channel_id = $channel.channel_id
  }
  $bPostReactionMatches = Items($bPosts.posts) | Where-Object {
    $_.post_id -eq $post.post_id -and $_.reaction_count -ge 1
  }
  (Items $bPostReactionMatches).Count -gt 0
} $FederationWaitSeconds "B did not project channel post reaction count")

[void](Wait-Until {
  $bComments = P2P $BBase "command" $bAuth.access_token "channels.comments.list" @{
    post_id = $post.post_id
  }
  $bCommentReactionMatches = Items($bComments.comments) | Where-Object {
    $_.comment_id -eq $comment.comment_id -and $_.reaction_count -ge 1
  }
  (Items $bCommentReactionMatches).Count -gt 0
} $FederationWaitSeconds "B did not project channel comment reaction count")

$myComments = P2P $ABase "command" $aAuth.access_token "channels.my_comments" @{
  channel_id = $channel.channel_id
}
Assert ((Items $myComments.comments).Count -gt 0) "channels.my_comments returned no owner comments"

$myReactions = P2P $ABase "command" $aAuth.access_token "channels.my_reactions" @{}
Assert ((Items $myReactions.reactions).Count -gt 0) "channels.my_reactions returned no owner reactions"

$commentRecall = P2P $ABase "command" $aAuth.access_token "channels.comments.recall" @{
  comment_id = $comment.comment_id
  reason = "comment smoke recall"
}
Assert ($commentRecall.status -eq "ok") "channels.comments.recall failed"

$postForRecall = P2P $ABase "command" $aAuth.access_token "channels.posts.create" @{
  channel_id = $channel.channel_id
  room_id = $channel.room_id
  body = "post recall $suffix"
}
$postRecall = P2P $ABase "command" $aAuth.access_token "channels.posts.recall" @{
  post_id = $postForRecall.post_id
  reason = "post smoke recall"
}
Assert ($postRecall.status -eq "ok") "channels.posts.recall failed"

$fav = P2P $ABase "command" $aAuth.access_token "favorites.add" @{
  event_id = $postEventID
  room_id = $postRoomID
  content = $post.body
  message_type = "text"
  origin_server_ts = $post.origin_server_ts
}
Assert ($fav.id -gt 0) "favorite add failed"

$favDuplicate = P2P $ABase "command" $aAuth.access_token "favorites.add" @{
  event_id = $postEventID
  room_id = $postRoomID
  content = "$($post.body) updated"
  message_type = "text"
  origin_server_ts = $post.origin_server_ts
}
Assert ($favDuplicate.id -eq $fav.id) "favorites.add did not reuse existing favorite id for the same event"

$favs = P2P $ABase "command" $aAuth.access_token "favorites.list" @{ message_type = "text" }
$matchingFavorites = @(Items($favs.favorites) | Where-Object { $_.id -eq $fav.id })
Assert ($matchingFavorites.Count -gt 0) "favorite list missing added favorite"
$sameEventFavorites = @(Items($favs.favorites) | Where-Object { $_.event_id -eq $postEventID -and $_.room_id -eq $postRoomID })
Assert ($sameEventFavorites.Count -eq 1) "favorites.list expected one favorite for event=$postEventID room=$postRoomID, got $($sameEventFavorites.Count)"

$favDelete = P2P $ABase "command" $aAuth.access_token "favorites.delete" @{
  id = $fav.id
}
Assert ($favDelete.status -eq "ok") "favorites.delete failed"

$fav2 = P2P $ABase "command" $aAuth.access_token "favorites.add" @{
  event_id = $post.event_id
  room_id = $channel.room_id
  content = "favorite batch $suffix"
  message_type = "text"
  origin_server_ts = $post.origin_server_ts
}
$favBatchDelete = P2P $ABase "command" $aAuth.access_token "favorites.delete_batch" @{
  ids = @($fav2.id)
}
Assert ($favBatchDelete.status -eq "ok") "favorites.delete_batch failed"

$call = P2P $ABase "command" $aAuth.access_token "calls.create" @{
  call_id = "call_$suffix"
  room_id = $channel.room_id
  media_type = "voice"
}
$call2 = P2P $ABase "command" $aAuth.access_token "calls.event" @{
  call_id = $call.call_id
  event = "connected"
}
Assert ($call2.state -eq "connected") "call event did not update state"

$callGet = P2P $ABase "command" $aAuth.access_token "calls.get" @{
  call_id = $call.call_id
}
Assert ($callGet.call_id -eq $call.call_id) "calls.get did not return created call"

$callList = P2P $ABase "command" $aAuth.access_token "calls.list" @{
  room_id = $channel.room_id
}
$callMatches = Items($callList.calls) | Where-Object { $_.call_id -eq $call.call_id }
Assert ((Items $callMatches).Count -gt 0) "calls.list missing created call"

$activeCalls = P2P $ABase "command" $aAuth.access_token "calls.active" @{
  room_id = $channel.room_id
}
$activeMatches = Items($activeCalls.calls) | Where-Object { $_.call_id -eq $call.call_id }
Assert ((Items $activeMatches).Count -gt 0) "calls.active missing connected call"

$incomingCall = P2P $ABase "command" $aAuth.access_token "calls.incoming" @{
  call_id = "incoming_$suffix"
  room_id = $channel.room_id
  media_type = "video"
}
Assert ($incomingCall.call_id -eq "incoming_$suffix") "calls.incoming did not create call"

$follow = P2P $ABase "command" $aAuth.access_token "follows.add" @{
  domain = "remote-$suffix.example"
}
$follows = P2P $ABase "command" $aAuth.access_token "follows.list" @{}
$matchingFollows = Items($follows.follows) | Where-Object { $_.domain -eq $follow.domain }
Assert ((Items $matchingFollows).Count -gt 0) "follow list missing added domain"

$followRemove = P2P $ABase "command" $aAuth.access_token "follows.remove" @{
  domain = $follow.domain
}
Assert ($followRemove.status -eq "ok") "follows.remove failed"

$report = P2P $ABase "command" $aAuth.access_token "reports.submit" @{
  reporter_domain = $AServerName
  reported_domain = $BServerName
  reason = "smoke report $suffix"
  target_type = 1
}
Assert ($report.id) "reports.submit did not return report id"

$group = P2P $ABase "command" $aAuth.access_token "groups.create" @{
  name = "Smoke Group $suffix"
  topic = "dual smoke group"
}
Assert ($group.room_id) "groups.create missing room_id"

$groupUpdated = P2P $ABase "command" $aAuth.access_token "groups.update" @{
  room_id = $group.room_id
  name = "Smoke Group Updated $suffix"
  topic = "updated"
}
Assert ($groupUpdated.name -eq "Smoke Group Updated $suffix") "groups.update did not persist name"
$groupInviteTitle = $groupUpdated.name

$groupList = P2P $ABase "command" $aAuth.access_token "groups.list" @{}
$groupMatches = Items($groupList.groups) | Where-Object { $_.room_id -eq $group.room_id }
Assert ((Items $groupMatches).Count -gt 0) "groups.list missing created group"

$groupMembers = P2P $ABase "command" $aAuth.access_token "groups.members" @{
  room_id = $group.room_id
}
Assert ((Items $groupMembers.members).Count -gt 0) "groups.members returned no members"

$groupMuted = P2P $ABase "command" $aAuth.access_token "groups.mute" @{
  room_id = $group.room_id
}
Assert ($groupMuted.muted -eq $true) "groups.mute did not mute group"

$groupUnmuted = P2P $ABase "command" $aAuth.access_token "groups.unmute" @{
  room_id = $group.room_id
}
Assert ($groupUnmuted.muted -eq $false) "groups.unmute did not unmute group"

$groupPolicy = P2P $ABase "command" $aAuth.access_token "groups.invite_policy.update" @{
  room_id = $group.room_id
  invite_policy = "owner"
}
Assert ($groupPolicy.invite_policy -eq "owner") "groups.invite_policy.update did not persist policy"

$groupInvite = P2P $ABase "command" $aAuth.access_token "groups.invite" @{
  room_id = $group.room_id
  user_id = $BOwnerMXID
  display_name = $bName2
}
Assert ((Items $groupInvite.members).Count -gt 0) "groups.invite did not invite B owner"

$bPendingGroupInvite = Wait-Until {
  $boot = P2P $BBase "command" $bAuth.access_token "sync.bootstrap" @{}
  $matches = Items($boot.pending.group_invites) | Where-Object {
    $_.id -eq $group.room_id -and $_.title -eq $groupInviteTitle
  }
  if ((Items $matches).Count -gt 0) {
    return $boot
  }
  return $null
} $FederationWaitSeconds "B did not receive pending group invite in sync.bootstrap"
$bPendingGroupInviteMatches = Items($bPendingGroupInvite.pending.group_invites) | Where-Object {
  $_.id -eq $group.room_id -and $_.title -eq $groupInviteTitle
}
Assert ((Items $bPendingGroupInviteMatches).Count -eq 1) "B pending group invite did not preserve group id/name"
$bPendingGroupMainMatches = Items($bPendingGroupInvite.groups) | Where-Object { $_.room_id -eq $group.room_id }
Assert ((Items $bPendingGroupMainMatches).Count -eq 0) "B sync.bootstrap groups should hide invited group before Matrix join"
$bGroupsBeforeJoin = P2P $BBase "command" $bAuth.access_token "groups.list" @{}
$bGroupsBeforeJoinMatches = Items($bGroupsBeforeJoin.groups) | Where-Object { $_.room_id -eq $group.room_id }
Assert ((Items $bGroupsBeforeJoinMatches).Count -eq 0) "B groups.list should hide invited group before Matrix join"

$groupJoin = P2P $BBase "command" $bAuth.access_token "groups.join" @{
  room_id = $group.room_id
  server_names = @($AServerName)
  display_name = $bName2
  avatar_url = $bAvatar2
}
Assert ($groupJoin.status -eq "ok") "groups.join did not join B owner after invite"

$bGroupsAfterJoin = P2P $BBase "command" $bAuth.access_token "groups.list" @{}
$bJoinedGroupMatches = Items($bGroupsAfterJoin.groups) | Where-Object { $_.room_id -eq $group.room_id }
Assert ((Items $bJoinedGroupMatches).Count -gt 0) "B groups.list missing joined remote group"

$groupMemberJoinedOnA = Wait-Until {
  $members = P2P $ABase "command" $aAuth.access_token "groups.members" @{
    room_id = $group.room_id
  }
  $matches = Items($members.members) | Where-Object {
    $_.user_id -eq $BOwnerMXID -and $_.membership -eq "join"
  }
  if ((Items $matches).Count -gt 0) {
    return $members
  }
  return $null
} $FederationWaitSeconds "A did not project B joined group membership"
Assert ((Items $groupMemberJoinedOnA.members).Count -gt 0) "A group members missing joined B"

$privateGrant = P2P $ABase "command" $aAuth.access_token "channels.invite_grant.create" @{
  room_id = $privateChannel.room_id
  share_room_id = $group.room_id
}
Assert ($privateGrant.grant_id) "channels.invite_grant.create did not return grant_id"

$bPrivateChannelNotice = Wait-Until {
  $boot = P2P $BBase "command" $bAuth.access_token "sync.bootstrap" @{}
  $matches = Items($boot.pending.channel_notices) | Where-Object { $_.id -eq $privateChannel.room_id }
  if ((Items $matches).Count -gt 0) {
    return $boot
  }
  return $null
} $FederationWaitSeconds "B did not receive private channel grant invite"
$bPrivateMainMatches = Items($bPrivateChannelNotice.channels) | Where-Object { $_.room_id -eq $privateChannel.room_id }
Assert ((Items $bPrivateMainMatches).Count -eq 0) "B sync.bootstrap channels should hide private invited channel before Matrix join"

$bPrivateJoin = P2P $BBase "command" $bAuth.access_token "channels.join" @{
  room_id = $privateChannel.room_id
  grant_id = $privateGrant.grant_id
  share_room_id = $group.room_id
  server_names = @($AServerName)
  display_name = $bName2
  avatar_url = $bAvatar2
}
Assert ($bPrivateJoin.status -eq "ok") "B private channel grant join failed"
Assert ($bPrivateJoin.room_id -eq $privateChannel.room_id) "B private channel grant join response missing room_id"
$bPrivateChannels = P2P $BBase "command" $bAuth.access_token "channels.list" @{}
$bPrivateChannelMatches = Items($bPrivateChannels.channels) | Where-Object { $_.room_id -eq $privateChannel.room_id }
Assert ((Items $bPrivateChannelMatches).Count -gt 0) "B channels.list missing private channel after grant join"

$groupMsgText = "hello group smoke $suffix"
$groupSent = Matrix-SendText $ABase $aAuth.access_token $group.room_id $groupMsgText
Assert ($groupSent.event_id) "A group Matrix send missing event_id"

$bGroupMessages = Wait-Until {
  $messages = Matrix-Messages $BBase $bAuth.access_token $group.room_id
  $roomMsgs = Items($messages.chunk)
  $match = $roomMsgs | Where-Object { $_.event_id -eq $groupSent.event_id -and $_.content.body -eq $groupMsgText }
  if ((Items $match).Count -gt 0) {
    return $messages
  }
  return $null
} $FederationWaitSeconds "B did not receive remote group Matrix message"

$groupOwnerMute = P2P $ABase "command" $aAuth.access_token "groups.member.mute" @{
  room_id = $group.room_id
  user_id = $AOwnerMXID
}
Assert ($groupOwnerMute.member.muted -eq $true) "groups.member.mute did not mute owner member"

$groupOwnerUnmute = P2P $ABase "command" $aAuth.access_token "groups.member.unmute" @{
  room_id = $group.room_id
  user_id = $AOwnerMXID
}
Assert ($groupOwnerUnmute.member.muted -eq $false) "groups.member.unmute did not unmute owner member"

$scratchChannel = P2P $ABase "command" $aAuth.access_token "channels.create" @{
  name = "Scratch Channel $suffix"
  visibility = "public"
  join_policy = "approval"
  channel_type = "chat"
}
Assert ($scratchChannel.room_id) "scratch channels.create missing room_id"

$scratchOwnerMute = P2P $ABase "command" $aAuth.access_token "channels.member.mute" @{
  room_id = $scratchChannel.room_id
  user_id = $AOwnerMXID
}
Assert ($scratchOwnerMute.member.muted -eq $true) "channels.member.mute did not mute owner member"

$scratchOwnerUnmute = P2P $ABase "command" $aAuth.access_token "channels.member.unmute" @{
  room_id = $scratchChannel.room_id
  user_id = $AOwnerMXID
}
Assert ($scratchOwnerUnmute.member.muted -eq $false) "channels.member.unmute did not unmute owner member"

$scratchMemberRemove = P2P $ABase "command" $aAuth.access_token "channels.member.remove" @{
  room_id = $channel.room_id
  user_id = $BOwnerMXID
}
Assert ($scratchMemberRemove.member.membership -eq "remove") "channels.member.remove did not mark ordinary remote member removed"

$removedChannelReapply = P2P $BBase "query" $null "channels.public.join_request" @{
  room_id = $channel.room_id
  user_id = $BOwnerMXID
  remote_node_base_url = $ARemoteNodeBaseURL
}
Assert ($removedChannelReapply.status -eq "rejected") "removed channel member reapply was not auto rejected"

$scratchOwnerRemove = P2P-Status $ABase "command" $aAuth.access_token "channels.member.remove" @{
  room_id = $scratchChannel.room_id
  user_id = $AOwnerMXID
}
Assert ($scratchOwnerRemove.StatusCode -eq 409) "channels.member.remove allowed removing channel owner instead of requiring dissolve"

$leaveChannel = P2P $ABase "command" $aAuth.access_token "channels.create" @{
  name = "Leave Channel $suffix"
  visibility = "public"
  join_policy = "open"
  channel_type = "chat"
}
$leaveResult = P2P-Status $ABase "command" $aAuth.access_token "channels.leave" @{
  room_id = $leaveChannel.room_id
}
Assert ($leaveResult.StatusCode -eq 409) "channels.leave allowed channel owner to leave instead of requiring dissolve"

$dissolvedChannel = P2P $ABase "command" $aAuth.access_token "channels.dissolve" @{
  room_id = $leaveChannel.room_id
}
Assert ($dissolvedChannel.status -eq "ok") "channels.dissolve did not return ok"
$channelsAfterDissolve = P2P $ABase "command" $aAuth.access_token "channels.list" @{}
$dissolvedChannelStillListed = Items($channelsAfterDissolve.channels) | Where-Object { $_.room_id -eq $leaveChannel.room_id }
Assert ((Items $dissolvedChannelStillListed).Count -eq 0) "channels.dissolve left dissolved channel in channels.list"

$deleteAccepted = P2P $BBase "command" $bAuth.access_token "contacts.delete" @{
  room_id = $contact.room_id
}
Assert ($deleteAccepted.status -eq "ok") "contacts.delete failed"

$deletedContactSend = Matrix-SendTextStatus $BBase $bAuth.access_token $contact.room_id "message after deleted contact $suffix"
Assert ($deletedContactSend.StatusCode -eq 403) "deleted contact room allowed Matrix sending a non-friend message"

$deletedContactRequest = P2P $BBase "command" $bAuth.access_token "contacts.request" @{
  mxid = $AOwnerMXID
  display_name = $aName
  remote_node_base_url = "$ABase/_p2p"
}
Assert ($deletedContactRequest.status -eq "accepted") "deleted contact peer request did not restore the retained direct room"
Assert ($deletedContactRequest.room_id -eq $contact.room_id) "deleted contact peer request created a replacement direct room"

$selfContactRequest = P2P-Status $ABase "command" $aAuth.access_token "contacts.request" @{
  mxid = $AOwnerMXID
  display_name = $aName
}
Assert ($selfContactRequest.StatusCode -eq 400) "contacts.request allowed owner to add itself"

$deleteBAgain = P2P $BBase "command" $bAuth.access_token "contacts.delete" @{
  room_id = $contact.room_id
}
Assert ($deleteBAgain.status -eq "ok") "contacts.delete failed before both-deleted re-request"

$deleteAContact = P2P $ABase "command" $aAuth.access_token "contacts.delete" @{
  room_id = $contact.room_id
}
Assert ($deleteAContact.status -eq "ok") "peer contacts.delete failed before both-deleted re-request"

$bothDeletedRequest = P2P $BBase "command" $bAuth.access_token "contacts.request" @{
  mxid = $AOwnerMXID
  display_name = $aName
  remote_node_base_url = "$ABase/_p2p"
}
Assert ($bothDeletedRequest.status -eq "pending_outbound") "both-deleted contact request did not create a fresh pending request"
Assert ($bothDeletedRequest.room_id -ne $contact.room_id) "both-deleted contact request reused the old direct room"

$groupMemberRemove = P2P $ABase "command" $aAuth.access_token "groups.member.remove" @{
  room_id = $group.room_id
  user_id = $BOwnerMXID
}
Assert ($groupMemberRemove.member.membership -eq "remove") "groups.member.remove did not mark ordinary remote member removed"

$removedGroupRejoin = P2P-Status $ABase "command" $aAuth.access_token "groups.join" @{
  room_id = $group.room_id
  user_id = $BOwnerMXID
}
Assert ($removedGroupRejoin.StatusCode -eq 403) "removed group member rejoin was not rejected"

$groupOwnerRemove = P2P-Status $ABase "command" $aAuth.access_token "groups.member.remove" @{
  room_id = $group.room_id
  user_id = $AOwnerMXID
}
Assert ($groupOwnerRemove.StatusCode -eq 409) "groups.member.remove allowed removing group owner instead of requiring dissolve"

$leaveGroup = P2P $ABase "command" $aAuth.access_token "groups.create" @{
  name = "Leave Group $suffix"
  topic = "leave smoke"
}
$groupLeave = P2P-Status $ABase "command" $aAuth.access_token "groups.leave" @{
  room_id = $leaveGroup.room_id
}
Assert ($groupLeave.StatusCode -eq 409) "groups.leave allowed group owner to leave instead of requiring dissolve"

$dissolvedGroup = P2P $ABase "command" $aAuth.access_token "groups.dissolve" @{
  room_id = $leaveGroup.room_id
}
Assert ($dissolvedGroup.status -eq "ok") "groups.dissolve did not return ok"
$groupsAfterDissolve = P2P $ABase "command" $aAuth.access_token "groups.list" @{}
$dissolvedGroupStillListed = Items($groupsAfterDissolve.groups) | Where-Object { $_.room_id -eq $leaveGroup.room_id }
Assert ((Items $dissolvedGroupStillListed).Count -eq 0) "groups.dissolve left dissolved group in groups.list"

$agentStatus = P2P $ABase "command" $aAuth.access_token "agent.status" @{}
Assert ($agentStatus.configured -eq $true) "access token could not read agent.status"

$bootstrapAgain = P2P $ABase "query" $null "portal.bootstrap" @{
  password = $aCred.password
  device_id = "SMOKEBOOT$suffix"
}
Assert $bootstrapAgain.access_token "portal.bootstrap did not return access_token"
Assert ($bootstrapAgain.initialized -eq $false) "portal.bootstrap should report initialized=false before password change"

$bootstrapKeysUpload = Matrix-KeysUpload $ABase $bootstrapAgain.access_token
Assert ($null -ne $bootstrapKeysUpload.one_time_key_counts) "Matrix keys/upload failed after portal.bootstrap"

$newPassword = (Get-Random -Minimum 10000000 -Maximum 99999999).ToString()
$passwordChanged = P2P $ABase "command" $bootstrapAgain.access_token "portal.password" @{
  old_password = $aCred.password
  new_password = $newPassword
  device_id = "SMOKEPASS$suffix"
}
Assert $passwordChanged.access_token "portal.password did not return rotated access_token"
Assert ($passwordChanged.initialized -eq $true) "portal.password did not preserve initialized=true"

$passwordKeysUpload = Matrix-KeysUpload $ABase $passwordChanged.access_token
Assert ($null -ne $passwordKeysUpload.one_time_key_counts) "Matrix keys/upload failed after portal.password"

$oldPasswordAuth = P2P-Status $ABase "query" $null "portal.auth" @{
  password = $aCred.password
  device_id = "SMOKEOLD$suffix"
}
Assert ($oldPasswordAuth.StatusCode -eq 401) "portal.auth accepted old password after portal.password"

$newPasswordAuth = P2P $ABase "query" $null "portal.auth" @{
  password = $newPassword
  device_id = "SMOKENEW$suffix"
}
Assert $newPasswordAuth.access_token "portal.auth rejected new password after portal.password"
Assert ($newPasswordAuth.initialized -eq $true) "portal.auth with new password did not report initialized=true"

$newPasswordKeysUpload = Matrix-KeysUpload $ABase $newPasswordAuth.access_token
Assert ($null -ne $newPasswordKeysUpload.one_time_key_counts) "Matrix keys/upload failed after portal.auth with new password"

Assert-AllBackendActionsCovered

$digest = ""
try {
  $digest = docker image inspect direxio/message-server:latest --format "{{index .RepoDigests 0}}"
} catch {
  $digest = "unavailable"
}

[pscustomobject]@{
  status = "ok"
  suffix = $suffix
  contact_room = $contact.room_id
  channel_id = $channel.channel_id
  channel_room = $channel.room_id
  a_member_rows = (Items $aMembersAfterProfile.members).Count
  b_projected_message_rooms = (Items $bMessage.rooms).Count
  api_actions_checked = (Items $apiList.items).Count
  p2p_actions_checked = $script:ActionsSeen.Keys.Count
  docker_image = $digest
} | ConvertTo-Json -Depth 10
