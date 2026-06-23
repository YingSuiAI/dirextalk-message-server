# Let Agent Token Bootstrap Matrix Session

Direxio agent tooling will not require Developer Operators to provide or separately log in for a Matrix access token. A trusted Agent token can be used to obtain the Matrix session needed for CLI Matrix workflows, allowing the CLI to initialize Matrix connectivity, send messages, and receive messages from the configured Direxio domain with only `DIREXIO_DOMAIN` and `DIREXIO_AGENT_TOKEN`.

The Matrix access token is an internal CLI credential and must not be printed or exposed as a normal command result.

This is an Agent/API permission boundary and must be implemented through the P2P action contract with explicit permissions, tests, Postman coverage, and API change notes when the server behavior is added.
