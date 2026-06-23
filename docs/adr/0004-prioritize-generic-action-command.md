# Prioritize a Generic Action Command

Status: superseded by ADR-0007

The first CLI interface will prioritize a generic action command that sends `{action, params}` to the existing P2P body-action API. This keeps the tool aligned with the server contract while the 85-action product surface is still changing, and avoids prematurely designing a large domain-specific command tree.
