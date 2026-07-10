#!/usr/bin/env python3
import argparse
import base64
import hashlib
import json
import os
import pathlib
import sys
import tempfile


def write_atomic(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    config = json.loads(pathlib.Path(args.config).read_text(encoding="utf-8"))
    sources = {}
    for edge in config["upgrade_edges"]:
        for identity in edge["from_image_digests"]:
            if config["source_test_modes"][identity] == "offline_import":
                sources[identity] = edge["from_version"]
    raw_payload = sys.stdin.read()
    try:
        payload = json.loads(raw_payload) if raw_payload else {}
    except Exception as exc:
        raise SystemExit(f"offline attestation input is invalid JSON: {exc}")
    if not isinstance(payload, dict) or set(payload) != set(sources):
        raise SystemExit("offline attestation input must cover every and only offline source identity")
    output = pathlib.Path(args.output)
    for identity, encoded in payload.items():
        if not isinstance(encoded, str) or len(encoded) > 131072:
            raise SystemExit("offline attestation base64 value is invalid")
        try:
            data = base64.b64decode(encoded, validate=True)
        except Exception as exc:
            raise SystemExit(f"offline attestation base64 is invalid: {exc}")
        if not data or len(data) > 65536:
            raise SystemExit("offline attestation size is invalid")
        name = f"release-attestation-{sources[identity].removeprefix('v')}-{identity.removeprefix('sha256:')}.json"
        path = output / name
        write_atomic(path, data)
        checksum = hashlib.sha256(data).hexdigest().encode() + b"  " + name.encode() + b"\n"
        write_atomic(pathlib.Path(str(path) + ".sha256"), checksum)


if __name__ == "__main__":
    main()
