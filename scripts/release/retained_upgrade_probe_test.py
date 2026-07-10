#!/usr/bin/env python3
import importlib.util
import pathlib
import unittest
from unittest import mock


MODULE_PATH = pathlib.Path(__file__).with_name("retained_upgrade_probe.py")
SPEC = importlib.util.spec_from_file_location("retained_upgrade_probe", MODULE_PATH)
PROBE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PROBE)


class WaitHealthTest(unittest.TestCase):
    @mock.patch.object(PROBE.time, "sleep", return_value=None)
    @mock.patch.object(PROBE, "request", return_value={"status": "ok"})
    def test_status_only_is_limited_to_explicit_legacy_source(self, _request, _sleep):
        PROBE.wait("http://unused", "v0.15.2", allow_status_only=True)
        with self.assertRaises(RuntimeError):
            PROBE.wait("http://unused", "v0.15.2")
        with self.assertRaises(RuntimeError):
            PROBE.wait("http://unused", "v1.0.0", allow_status_only=True)

    @mock.patch.object(PROBE.time, "sleep", return_value=None)
    @mock.patch.object(PROBE, "request", return_value={"status": "ok", "version": "v1.0.0"})
    def test_formal_target_still_requires_matching_version(self, _request, _sleep):
        PROBE.wait("http://unused", "v1.0.0")


if __name__ == "__main__":
    unittest.main()
