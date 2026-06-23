# Cover P2P and Matrix With Domain CLI Commands

Direxio's CLI will expose first-class domain commands for the full relevant service surface, including P2P product actions and Matrix client APIs under `/_matrix/*`. A generic action command may remain as a fallback, but the primary user workflow is domain commands for initialization, connection/session setup, message send/receive, and product operations. Operators configure only the site domain plus Agent token; the CLI derives `/_p2p` and `/_matrix` routes internally.
