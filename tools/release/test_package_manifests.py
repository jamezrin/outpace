import importlib.util
import json
from pathlib import Path
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
            manifests.render("v1.2.3", text, root)
            formula = (root / "Formula" / "outpace.rb").read_text()
            scoop = json.loads((root / "bucket" / "outpace.json").read_text())
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
            manifests.render("v1.2.3", text, Path(tmp))

    def test_invalid_tag_rejected(self):
        with self.assertRaises(ValueError):
            manifests.parse_tag("v1.2.3-rc1")

    def test_committed_manifests_are_generated_output(self):
        # Formula/ and bucket/ must be exactly what the generator writes for their version.
        root = manifests.ROOT
        scoop = json.loads((root / "bucket" / "outpace.json").read_text())
        version = scoop["version"]
        sums = {}
        for arch, entry in scoop["architecture"].items():
            sums[f"outpace-{version}-{manifests.SCOOP[arch]}.zip"] = entry["hash"]
        formula = (root / "Formula" / "outpace.rb").read_text()
        for key, target in manifests.BREW.items():
            name = f"outpace-{version}-{target}.tar.gz"
            block = formula.split(name, 1)[1]
            sums[name] = block.split('sha256 "', 1)[1].split('"', 1)[0]
        text = "".join(f"{digest}  {name}\n" for name, digest in sums.items())
        with tempfile.TemporaryDirectory() as tmp:
            manifests.render(f"v{version}", text, Path(tmp))
            self.assertEqual((Path(tmp) / "Formula" / "outpace.rb").read_text(), formula)
            self.assertEqual(
                (Path(tmp) / "bucket" / "outpace.json").read_text(),
                (root / "bucket" / "outpace.json").read_text(),
            )


if __name__ == "__main__":
    unittest.main()
