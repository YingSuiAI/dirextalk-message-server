# Standardize Agent Tool JSON Output

Dirextalk's CLI will print successful responses as pretty JSON by default and compact JSON when `--raw` is set. Failures will be written to stderr with non-zero exit codes so Developer Operators, CI scripts, and intelligent agent tools can consume stdout as reliable machine-readable result data.
