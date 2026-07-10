#!/usr/bin/env python3
import argparse
import datetime
import hashlib
import json
import os
import pathlib
import re
import tempfile


CHECKS = ["portal_login", "profile_persistence", "matrix_room_message_persistence", "target_health_version"]
DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
VERSION = re.compile(r"v(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)")


def sha256_file(path):
    return "sha256:" + hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()


def canonical(value):
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode()


def expected(args, tested_at):
    return {
        "attestation_version": 1,
        "checks": CHECKS,
        "from_version": args.from_version,
        "release_config_sha256": sha256_file(args.release_config),
        "release_version": args.release_version,
        "result": "passed",
        "runner_sha256": sha256_file(args.runner),
        "source_image_identity": args.source_identity,
        "source_mode": args.source_mode,
        "target_commit": args.target_commit,
        "target_image": args.target_image,
        "target_image_id": args.target_image_id,
        "test_method": "ubuntu24_compose_portal_profile_matrix_retained_data",
        "tested_at": tested_at,
    }


def validate_args(args):
    if not VERSION.fullmatch(args.from_version) or not VERSION.fullmatch(args.release_version):
        raise SystemExit("attestation versions are invalid")
    if not DIGEST.fullmatch(args.source_identity) or not DIGEST.fullmatch(args.target_image_id):
        raise SystemExit("attestation image identity is invalid")
    if not re.fullmatch(r"[0-9a-f]{40}", args.target_commit):
        raise SystemExit("attestation target commit is invalid")
    if args.source_mode not in {"registry", "offline_import"}:
        raise SystemExit("attestation source mode is invalid")
    if args.target_image != f"dirextalk/message-server:{args.release_version}":
        raise SystemExit("attestation target image is invalid")


def create(args):
    validate_args(args)
    tested_at = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    data = canonical(expected(args, tested_at))
    destination = pathlib.Path(args.attestation)
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=destination.name + ".", dir=destination.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, destination)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    checksum = pathlib.Path(str(destination) + ".sha256")
    checksum.write_text(hashlib.sha256(data).hexdigest() + "  " + destination.name + "\n", encoding="ascii", newline="\n")
    os.chmod(checksum, 0o600)


def verify(args):
    validate_args(args)
    path = pathlib.Path(args.attestation)
    raw = path.read_bytes()
    try:
        value = json.loads(raw)
    except Exception as exc:
        raise SystemExit(f"invalid retained-upgrade attestation: {exc}")
    tested_at = value.get("tested_at")
    if not isinstance(tested_at, str) or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z", tested_at):
        raise SystemExit("attestation tested_at is invalid")
    wanted = expected(args, tested_at)
    if value != wanted or raw != canonical(wanted):
        raise SystemExit("retained-upgrade attestation does not bind the current release inputs")
    checksum_path = pathlib.Path(str(path) + ".sha256")
    checksum = checksum_path.read_bytes()
    wanted_checksum = hashlib.sha256(raw).hexdigest().encode() + b"  " + path.name.encode() + b"\n"
    if checksum != wanted_checksum:
        raise SystemExit("retained-upgrade attestation checksum mismatch")


def parser():
    result = argparse.ArgumentParser()
    result.add_argument("command", choices=("create", "verify"))
    result.add_argument("--attestation", required=True)
    result.add_argument("--from-version", required=True)
    result.add_argument("--source-identity", required=True)
    result.add_argument("--source-mode", required=True)
    result.add_argument("--release-version", required=True)
    result.add_argument("--target-commit", required=True)
    result.add_argument("--target-image", required=True)
    result.add_argument("--target-image-id", required=True)
    result.add_argument("--release-config", required=True)
    result.add_argument("--runner", required=True)
    return result


if __name__ == "__main__":
    arguments = parser().parse_args()
    if arguments.command == "create":
        create(arguments)
    else:
        verify(arguments)
