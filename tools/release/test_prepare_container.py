import hashlib
import importlib.util
import io
from pathlib import Path
import tarfile
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("prepare_container", Path(__file__).with_name("prepare_container.py"))
prepare = importlib.util.module_from_spec(spec)
spec.loader.exec_module(prepare)


class PrepareTests(unittest.TestCase):
    def fixture(self, root, member_type=tarfile.REGTYPE):
        for target, extension in prepare.TARGETS.items():
            archive = root / f"outpace-0.3.0-{target}.{extension}"
            if target in prepare.LINUX:
                with tarfile.open(archive, "w:gz") as tar:
                    member = tarfile.TarInfo(f"outpace-0.3.0-{target}/outpace")
                    member.type = member_type
                    member.mode = 0o755
                    member.size = 3 if member_type == tarfile.REGTYPE else 0
                    tar.addfile(member, io.BytesIO(b"bin") if member.size else None)
            else:
                archive.write_bytes(b"other platform archive")
        self.checksums(root)

    def checksums(self, root):
        (root / "SHA256SUMS").write_text("".join(
            f"{hashlib.sha256(p.read_bytes()).hexdigest()}  {p.name}\n"
            for p in sorted(root.iterdir()) if p.name != "SHA256SUMS"
        ))

    def test_prepares_only_linux_executables(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.fixture(root)
            prepare.prepare("v0.3.0", root, root / "context")
            self.assertEqual(sorted(str(p.relative_to(root / "context")) for p in (root / "context").rglob("outpace")),
                             ["amd64/outpace", "arm-v7/outpace", "arm64/outpace"])

    def test_corrupt_archive_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.fixture(root)
            next(root.glob("*.zip")).write_bytes(b"corrupt")
            with self.assertRaises(ValueError):
                prepare.prepare("v0.3.0", root, root / "context")

    def test_missing_target_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.fixture(root)
            next(root.glob("*.zip")).unlink()
            self.checksums(root)
            with self.assertRaises(ValueError):
                prepare.prepare("v0.3.0", root, root / "context")

    def test_symlink_binary_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.fixture(root, tarfile.SYMTYPE)
            with self.assertRaises(ValueError):
                prepare.prepare("v0.3.0", root, root / "context")

    def test_duplicate_checksum_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.fixture(root)
            sums = root / "SHA256SUMS"
            sums.write_text(sums.read_text() + sums.read_text().splitlines()[0] + "\n")
            with self.assertRaises(ValueError):
                prepare.prepare("v0.3.0", root, root / "context")

    def test_missing_executable_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.fixture(root)
            target = next(iter(prepare.LINUX))
            with tarfile.open(root / f"outpace-0.3.0-{target}.tar.gz", "w:gz"):
                pass
            self.checksums(root)
            with self.assertRaises(ValueError):
                prepare.prepare("v0.3.0", root, root / "context")

    def test_invalid_tag_rejected(self):
        with self.assertRaises(ValueError):
            prepare.prepare("v0.3.0;echo unsafe", Path("unused"), Path("unused"))


if __name__ == "__main__":
    unittest.main()
