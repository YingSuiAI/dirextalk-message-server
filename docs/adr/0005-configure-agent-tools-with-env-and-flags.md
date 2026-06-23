# Configure Agent Tools With Environment Variables and Flags

The first agent tools will read `DIREXIO_DOMAIN` and `DIREXIO_AGENT_TOKEN` from environment variables by default, while allowing explicit command-line flags to override them for one-off scripts. The tools derive P2P and Matrix route bases internally and will not store profiles or credentials in the first version, avoiding cross-platform secret-storage complexity and accidental Agent token persistence.
