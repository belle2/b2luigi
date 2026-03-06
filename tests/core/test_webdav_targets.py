import os
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import MagicMock, patch
import b2luigi
from b2luigi import WebDAVSystem
from b2luigi.core.settings import _no_value
import b2luigi.core.remote_target.webdav
from b2luigi.core.remote_target.webdav import ensure_requests_ca_bundle


class TestEnsureRequestsCABundle(unittest.TestCase):
    def setUp(self):
        # avoid leaking the tempdir reference between tests
        if hasattr(ensure_requests_ca_bundle, "_temp_dir"):
            delattr(ensure_requests_ca_bundle, "_temp_dir")
        self.old = os.environ.pop("REQUESTS_CA_BUNDLE", None)

    def tearDown(self) -> None:
        if self.old is None:
            os.environ.pop("REQUESTS_CA_BUNDLE", None)
        else:
            os.environ["REQUESTS_CA_BUNDLE"] = self.old

    def test_sets_env_if_unset(self):
        with tempfile.TemporaryDirectory() as td:
            extra = Path(td) / "extra.pem"
            extra.write_text("CERT")

            ensure_requests_ca_bundle(str(extra))
            self.assertEqual(os.environ["REQUESTS_CA_BUNDLE"], str(extra.resolve()))

    def test_raises_if_extra_missing(self):
        with tempfile.TemporaryDirectory() as td:
            missing = Path(td) / "missing.pem"
            with self.assertRaises(FileNotFoundError):
                ensure_requests_ca_bundle(str(missing))

    def test_noop_if_same_path(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "bundle.pem"
            p.write_text("CERT")

            os.environ["REQUESTS_CA_BUNDLE"] = str(p)
            ensure_requests_ca_bundle(str(p))
            self.assertEqual(os.environ["REQUESTS_CA_BUNDLE"], str(p.resolve()))

    def test_merges_file_and_file(self):
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            existing = td / "existing.pem"
            extra = td / "extra.pem"

            existing.write_text("EXISTING")
            extra.write_text("EXTRA")

            os.environ["REQUESTS_CA_BUNDLE"] = str(existing)
            out_dir = ensure_requests_ca_bundle(str(extra))
            bundle_path = Path(os.environ["REQUESTS_CA_BUNDLE"])
            self.assertEqual(bundle_path.resolve(), out_dir.resolve())
            self.assertTrue(hasattr(ensure_requests_ca_bundle, "_temp_dir"))

            shutil.rmtree(out_dir)

    def test_merges_dir_and_dir(self):
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            existing = td / "existing"
            extra = td / "extra"
            existing.mkdir()
            extra.mkdir()

            for name in ["a.txt", "b.txt", "c.txt"]:
                (existing / name).touch()

            for name in ["d.txt", "e.txt"]:
                (extra / name).touch()

            os.environ["REQUESTS_CA_BUNDLE"] = str(existing)
            out_dir = ensure_requests_ca_bundle(str(extra))
            bundle_path = Path(os.environ["REQUESTS_CA_BUNDLE"])
            self.assertEqual(bundle_path.resolve(), out_dir.resolve())

            files = {file.name for file in out_dir.iterdir()}
            expected = {"a.txt", "b.txt", "c.txt", "d.txt", "e.txt"}
            self.assertEqual(files, expected)

            shutil.rmtree(out_dir)


class TestWebDAVSystemInit(unittest.TestCase):
    def setUp(self):
        self.old_proxy = os.environ.pop("X509_USER_PROXY", None)
        self.old_certs = os.environ.pop("X509_CERT_DIR", None)

    def tearDown(self) -> None:
        if self.old_proxy is None:
            os.environ.pop("X509_USER_PROXY", None)
        else:
            os.environ["X509_USER_PROXY"] = self.old_proxy
        if self.old_certs is None:
            os.environ.pop("X509_CERT_DIR", None)
        else:
            os.environ["X509_CERT_DIR"] = self.old_certs

    def test_init_requires_proxy(self):
        with self.assertRaises(ValueError):
            WebDAVSystem("https://example/")

    def test_init_requires_cert_dir(self):
        def fake_get_setting(k, default=_no_value):
            if k == "X509_USER_PROXY":
                return "/tmp/proxy"
            if k == "X509_CERT_DIR":
                return _no_value
            return default

        with patch("b2luigi.core.remote_target.webdav.get_setting", side_effect=fake_get_setting):
            with self.assertRaises(ValueError):
                WebDAVSystem("https://example/")

    def test_init_sets_cert_and_calls_ensure(self):
        def fake_get_setting(k, default=None):
            if k == "X509_USER_PROXY":
                return "/tmp/proxy"
            if k == "X509_CERT_DIR":
                return "/certs"
            return default

        with patch.object(b2luigi.core.remote_target.webdav, "ensure_requests_ca_bundle") as ensure, patch(
            "b2luigi.core.remote_target.webdav.get_setting", side_effect=fake_get_setting
        ):
            fs = WebDAVSystem("https://host/")
            self.assertEqual(fs.client.session.cert, "/tmp/proxy")
            ensure.assert_called_once_with("/certs")


class TestWebDAVSystem(unittest.TestCase):
    def setUp(self):
        self.p_client = patch("b2luigi.core.remote_target.webdav.Client")
        self.p_get_setting = patch("b2luigi.core.remote_target.webdav.get_setting")
        self.p_ensure = patch("b2luigi.core.remote_target.webdav.ensure_requests_ca_bundle")

        self.Client = self.p_client.start()
        self.get_setting = self.p_get_setting.start()

        def fake_get_setting(k, default=None):
            if k == "X509_USER_PROXY":
                return "/tmp/proxy"
            if k == "X509_CERT_DIR":
                return "/certs"
            return default

        self.get_setting.side_effect = fake_get_setting
        self.ensure = self.p_ensure.start()

        self.client = MagicMock()
        self.Client.return_value = self.client
        self.system = WebDAVSystem("https://example/")

    def tearDown(self):
        self.p_client.stop()
        self.p_get_setting.stop()
        self.p_ensure.stop()

    def test_exists(self):
        self.client.check.return_value = True
        self.system.exists("foo/bar")
        self.client.check.assert_called_once_with("foo/bar")

    def test_locate(self):
        self.client.check.return_value = False
        self.assertFalse(self.system.locate("foo/bar"))
        self.client.check.assert_called_once_with("foo/bar")

    def test_listdir(self):
        self.client.list.return_value = ["a", "b"]
        out = self.system.listdir("some/dir")
        self.assertEqual(out, ["a", "b"])
        self.client.list.assert_called_once_with("some/dir")

    def test_mkdir(self):
        self.system.mkdir("new/dir")
        self.client.mkdir.assert_called_once_with(remote_path="new/dir")

    def test_move(self):
        self.system.move("src", "dst")
        self.client.move.assert_called_once_with(remote_path_from="src", remote_path_to="dst")

    def test_remove(self):
        self.system.remove("file.txt")
        self.client.clean.assert_called_once_with(remote_path="file.txt")

    def test_remove_dir_calls_remove(self):
        with patch.object(self.system, "remove") as rm:
            self.system.remove_dir("some/dir")
            rm.assert_called_once_with(path="some/dir")

    def test_copy_file_to_remote_no_force(self):
        self.system.copy_file_to_remote("local.txt", "remote.txt", force=False)
        self.client.check.assert_not_called()
        self.client.clean.assert_not_called()
        self.client.upload_sync.assert_called_once_with(remote_path="remote.txt", local_path="local.txt")

    def test_copy_file_to_remote_force_remote_exists(self):
        self.client.check.return_value = True
        self.system.copy_file_to_remote("local.txt", "remote.txt", force=True)
        self.client.check.assert_called_once_with("remote.txt")
        self.client.clean.assert_called_once_with(remote_path="remote.txt")
        self.client.upload_sync.assert_called_once_with(remote_path="remote.txt", local_path="local.txt")

    def test_copy_file_to_remote_force_remote_missing(self):
        self.client.check.return_value = False
        self.system.copy_file_to_remote("local.txt", "remote.txt", force=True)
        self.client.check.assert_called_once_with("remote.txt")
        self.client.clean.assert_not_called()
        self.client.upload_sync.assert_called_once_with(remote_path="remote.txt", local_path="local.txt")

    def test_copy_file_from_remote_no_force_existing_local(self):
        with tempfile.TemporaryDirectory() as td:
            local = Path(td) / "x.txt"
            local.write_text("old")

            self.system.copy_file_from_remote("remote.txt", str(local), force=False)

            self.assertTrue(local.exists())  # should not unlink
            self.client.download_sync.assert_called_once_with(remote_path="remote.txt", local_path=str(local))

    def test_copy_file_from_remote_force_existing_local(self):
        with tempfile.TemporaryDirectory() as td:
            local = Path(td) / "x.txt"
            local.write_text("old")

            self.system.copy_file_from_remote("remote.txt", str(local), force=True)

            self.assertFalse(local.exists())  # should unlink
            self.client.download_sync.assert_called_once_with(remote_path="remote.txt", local_path=str(local))

    def test_copy_file_from_remote_force_missing_local(self):
        with tempfile.TemporaryDirectory() as td:
            local = Path(td) / "x.txt"
            self.assertFalse(local.exists())

            self.system.copy_file_from_remote("remote.txt", str(local), force=True)

            self.client.download_sync.assert_called_once_with(remote_path="remote.txt", local_path=str(local))

    def test_copy_dir_from_remote_delegates_to_copy_file_from_remote(self):
        with patch.object(self.system, "copy_file_from_remote") as cffr:
            self.system.copy_dir_from_remote("remote/dir", "local/dir", force=True)
            cffr.assert_called_once_with("remote/dir", "local/dir", force=True)


if __name__ == "__main__":
    unittest.main()
