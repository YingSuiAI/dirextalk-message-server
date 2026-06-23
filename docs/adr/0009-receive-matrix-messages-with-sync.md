# Receive Matrix Messages With Sync

Direxio's CLI will support both one-shot message retrieval and continuous listening, backed first by Matrix Client-Server sync rather than a new P2P message endpoint. Continuous listen output should be event-stream friendly, while one-shot commands keep the standard JSON result contract.
