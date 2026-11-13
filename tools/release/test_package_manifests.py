import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("package_manifests", Path(__file__).with_name("package_manifests.py"))
manifests = importlib.util.module_from_spec(spec)
spec.loader.exec_module(manifests)

TARGETS = list(manifests.BREW.values()) + list(manifests.SCOOP.values())


def sums_for(version):
    lines = []
    for index, target in enumerate(sorted(TARGETS)):
        ext = "zip" if "windows" in target else "tar.gz"
        lines.append(f"{index:064x}  outpace-{version}-{target}.{ext}")
    lines.append(f"{'f' * 64}  outpace_{version}_amd64.deb")
    return "\n".join(lines) + "\n"


class ManifestTests(unittest.TestCase):
    def test_renders_every_platform_with_its_checksum(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            text = sums_for("1.2.3")
            sums = manifests.parse_sums(text)
            manifests.render("v1.2.3", text, root / "homebrew", root / "scoop")
            formula = (root / "homebrew" / "Formula" / "outpace.rb").read_text()
            scoop = json.loads((root / "scoop" / "bucket" / "outpace.json").read_text())
        # Homebrew scans the version from the URL; an explicit `version` fails `brew audit`.
        self.assertNotIn('version "', formula)
        for target in manifests.BREW.values():
            name = f"outpace-1.2.3-{target}.tar.gz"
            self.assertIn(f"/releases/download/v1.2.3/{name}", formula)
            self.assertIn(sums[name], formula)
        self.assertEqual(scoop["version"], "1.2.3")
        for arch, target in manifests.SCOOP.items():
            entry = scoop["architecture"][arch]
            self.assertEqual(entry["hash"], sums[f"outpace-1.2.3-{target}.zip"])
            self.assertEqual(entry["extract_dir"], f"outpace-1.2.3-{target}")
        self.assertEqual(scoop["autoupdate"]["hash"]["url"], "$baseurl/SHA256SUMS")

    def test_missing_platform_rejected(self):
        text = "\n".join(l for l in sums_for("1.2.3").splitlines() if "aarch64-apple-darwin" not in l)
        with tempfile.TemporaryDirectory() as tmp, self.assertRaises(ValueError):
            manifests.render("v1.2.3", text, Path(tmp) / "homebrew", Path(tmp) / "scoop")

    def test_invalid_tag_rejected(self):
        with self.assertRaises(ValueError):
            manifests.parse_tag("v1.2.3-rc1")

    def test_missing_scoop_checksum_leaves_both_repositories_untouched(self):
        text = "\n".join(l for l in sums_for("1.2.3").splitlines() if "windows" not in l)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaises(ValueError):
                manifests.render("v1.2.3", text, root / "homebrew", root / "scoop")
            self.assertEqual(list(root.iterdir()), [])

    def test_cli_writes_only_to_explicit_catalog_repositories(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            sums = root / "SHA256SUMS"
            sums.write_text(sums_for("1.2.3"))
            script = Path(manifests.__file__).resolve()
            result = subprocess.run([
                sys.executable, str(script), "v1.2.3", str(sums),
                "--homebrew-root", str(root / "homebrew"),
                "--scoop-root", str(root / "scoop"),
            ], cwd=root, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            formula = (root / "homebrew" / "Formula" / "outpace.rb").read_text()
            scoop = json.loads((root / "scoop" / "bucket" / "outpace.json").read_text())
            expected = "https://github.com/outpace-stream/outpace-engine"
            self.assertIn(expected + "/releases/download/v1.2.3/", formula)
            self.assertEqual(scoop["homepage"], expected)
            self.assertEqual(scoop["checkver"]["github"], expected)
            self.assertFalse((root / "Formula").exists())
            self.assertFalse((root / "bucket").exists())

    def test_cli_requires_explicit_catalog_repositories(self):
        result = subprocess.run([
            sys.executable, str(Path(manifests.__file__).resolve()), "v1.2.3", "SHA256SUMS",
        ], capture_output=True, text=True)
        self.assertEqual(result.returncode, 2)
        self.assertIn("--homebrew-root", result.stderr)
        self.assertIn("--scoop-root", result.stderr)


if __name__ == "__main__":
    unittest.main()
