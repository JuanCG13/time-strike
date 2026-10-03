#!/usr/bin/env python3
import json
import re
import sys
import tempfile
import tomllib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
VERSION = r"\d+\.\d+\.\d+"
RELEASE = re.compile(rf"^## \[({VERSION})\] - (\d{{4}}-\d{{2}}-\d{{2}})$", re.MULTILINE)
MCP_VERSION = re.compile(
    rf'#\[tool_handler\(\s*name\s*=\s*"time-strike",\s*version\s*=\s*"({VERSION})"',
    re.DOTALL,
)
LATEST = re.compile(
    rf"## Latest changes\s+\*\*v({VERSION}):.*?"
    rf"\[changelog\]\(CHANGELOG\.md#([0-9]+---\d{{4}}-\d{{2}}-\d{{2}})\)",
    re.DOTALL,
)


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def validate(root: Path) -> list[str]:
    errors = []
    manifest = tomllib.loads(read(root / "Cargo.toml"))
    version = manifest.get("package", {}).get("version")
    if not isinstance(version, str) or not re.fullmatch(VERSION, version):
        return ["Cargo.toml package.version must be SemVer X.Y.Z"]

    lock = tomllib.loads(read(root / "Cargo.lock"))
    local_packages = [
        package
        for package in lock.get("package", [])
        if package.get("name") == "time-strike" and "source" not in package
    ]
    if len(local_packages) != 1:
        errors.append("Cargo.lock must contain exactly one local time-strike package")
    elif local_packages[0].get("version") != version:
        errors.append("Cargo.lock local package version differs from Cargo.toml")

    main = read(root / "src/main.rs")
    mcp_versions = MCP_VERSION.findall(main)
    if mcp_versions != [version]:
        errors.append("MCP server metadata version differs from Cargo.toml")

    changelog = read(root / "CHANGELOG.md")
    releases = RELEASE.findall(changelog)
    release_versions = [release_version for release_version, _ in releases]
    if release_versions.count(version) != 1:
        errors.append("CHANGELOG must contain exactly one release for Cargo.toml version")
        release_date = None
    else:
        release_date = next(date for release_version, date in releases if release_version == version)
    if not releases or releases[0][0] != version:
        errors.append("CHANGELOG newest release must match Cargo.toml version")
    parsed_versions = [tuple(map(int, release.split("."))) for release in release_versions]
    if parsed_versions != sorted(parsed_versions, reverse=True):
        errors.append("CHANGELOG releases must be in descending SemVer order")

    readme = read(root / "README.md")
    latest = LATEST.findall(readme)
    if len(latest) != 1:
        errors.append("README must contain one parseable Latest changes entry")
    else:
        latest_version, latest_anchor = latest[0]
        if latest_version != version:
            errors.append("README Latest changes version differs from Cargo.toml")
        if release_date is not None:
            expected_anchor = f"{version.replace('.', '')}---{release_date}"
            if latest_anchor != expected_anchor:
                errors.append("README changelog link does not target the current release")

    return errors


def write_fixture(root: Path, *, mcp_version: str = "1.2.3") -> None:
    (root / "src").mkdir()
    (root / "Cargo.toml").write_text(
        '[package]\nname = "time-strike"\nversion = "1.2.3"\n', encoding="utf-8"
    )
    (root / "Cargo.lock").write_text(
        'version = 4\n\n[[package]]\nname = "time-strike"\nversion = "1.2.3"\n',
        encoding="utf-8",
    )
    (root / "src/main.rs").write_text(
        f'#[tool_handler(\n    name = "time-strike",\n    version = "{mcp_version}",\n)]\n',
        encoding="utf-8",
    )
    (root / "README.md").write_text(
        "## Latest changes\n\n"
        "**v1.2.3:** verified release. See the "
        "[changelog](CHANGELOG.md#123---2026-10-03).\n",
        encoding="utf-8",
    )
    (root / "CHANGELOG.md").write_text(
        "## [Unreleased]\n\n## [1.2.3] - 2026-10-03\n\n## [1.2.2] - 2026-10-02\n",
        encoding="utf-8",
    )


def self_test() -> None:
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        write_fixture(root)
        assert validate(root) == []
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        write_fixture(root, mcp_version="1.2.2")
        assert "MCP server metadata version differs from Cargo.toml" in validate(root)


def main() -> int:
    self_test()
    errors = validate(ROOT)
    outcome = {
        "check": "version_consistency",
        "outcome": "failure" if errors else "success",
        "errors": errors,
    }
    print(json.dumps(outcome, separators=(",", ":")), file=sys.stderr if errors else sys.stdout)
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
