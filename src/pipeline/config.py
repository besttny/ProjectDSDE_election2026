from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError:  # pragma: no cover - only used in minimal verification envs
    yaml = None


@dataclass(frozen=True)
class ProjectConfig:
    """Runtime configuration resolved relative to the repository root."""

    root: Path
    data: dict[str, Any]

    @property
    def project(self) -> dict[str, Any]:
        return self.data.get("project", {})

    @property
    def paths(self) -> dict[str, str]:
        return self.data.get("paths", {})

    @property
    def ocr(self) -> dict[str, Any]:
        return self.data.get("ocr", {})

    @property
    def quality(self) -> dict[str, Any]:
        return self.data.get("quality", {})

    @property
    def outputs(self) -> dict[str, str]:
        return self.data.get("outputs", {})

    @property
    def province(self) -> str:
        return str(self.project.get("province", ""))

    @property
    def constituency_no(self) -> int:
        return int(self.project.get("constituency_no", 0))

    @property
    def expected_polling_stations(self) -> int:
        return int(self.project.get("expected_polling_stations", 0))

    def resolve(self, value: str | Path) -> Path:
        path = Path(value)
        return path if path.is_absolute() else self.root / path

    def path(self, key: str) -> Path:
        return self.resolve(self.paths[key])

    def output(self, key: str) -> Path:
        return self.resolve(self.outputs[key])

    def ensure_output_dirs(self) -> None:
        for key in [
            "raw_image_dir",
            "raw_ocr_dir",
            "parsed_dir",
            "processed_dir",
            "figures_dir",
            "reports_dir",
        ]:
            self.path(key).mkdir(parents=True, exist_ok=True)
        for value in self.outputs.values():
            self.resolve(value).parent.mkdir(parents=True, exist_ok=True)


def find_project_root(config_path: Path) -> Path:
    for candidate in [config_path.parent, *config_path.parents]:
        if (candidate / "README.md").exists() and (candidate / "requirements.txt").exists():
            return candidate
    return Path.cwd()


def _parse_scalar(value: str) -> Any:
    value = value.strip()
    if value.lower() in {"true", "false"}:
        return value.lower() == "true"
    if value.lower() in {"null", "none", "~"}:
        return None
    if value.startswith(("'", '"')) and value.endswith(("'", '"')):
        return value[1:-1]
    try:
        return int(value)
    except ValueError:
        return value


def _simple_yaml_load(text: str) -> dict[str, Any]:
    """Small fallback parser for this repo's simple config file.

    It supports nested mappings, scalar values, and scalar lists. Use PyYAML in
    normal environments; this path exists so smoke checks can run before setup.
    """

    raw_lines = [line.rstrip() for line in text.splitlines()]
    lines = [
        line
        for line in raw_lines
        if line.strip() and not line.lstrip().startswith("#")
    ]
    root: dict[str, Any] = {}
    stack: list[tuple[int, dict[str, Any] | list[Any]]] = [(-1, root)]

    for index, line in enumerate(lines):
        indent = len(line) - len(line.lstrip(" "))
        stripped = line.strip()
        while stack and indent <= stack[-1][0]:
            stack.pop()
        parent = stack[-1][1]

        if stripped.startswith("- "):
            if not isinstance(parent, list):
                raise ValueError("Invalid fallback YAML: list item under non-list parent")
            parent.append(_parse_scalar(stripped[2:]))
            continue

        key, separator, value = stripped.partition(":")
        if not separator:
            raise ValueError(f"Invalid fallback YAML line: {line}")
        key = key.strip()
        value = value.strip()
        if value:
            if not isinstance(parent, dict):
                raise ValueError("Invalid fallback YAML: mapping under list parent")
            parent[key] = _parse_scalar(value)
            continue

        next_container: dict[str, Any] | list[Any]
        next_line = ""
        for candidate in lines[index + 1 :]:
            if len(candidate) - len(candidate.lstrip(" ")) > indent:
                next_line = candidate.strip()
                break
        next_container = [] if next_line.startswith("- ") else {}
        if not isinstance(parent, dict):
            raise ValueError("Invalid fallback YAML: mapping under list parent")
        parent[key] = next_container
        stack.append((indent, next_container))

    return root


def load_config(config_path: str | Path) -> ProjectConfig:
    path = Path(config_path)
    if not path.is_absolute():
        path = Path.cwd() / path
    path = path.resolve()
    text = path.read_text(encoding="utf-8")
    data = yaml.safe_load(text) if yaml is not None else _simple_yaml_load(text)
    data = data or {}
    return ProjectConfig(root=find_project_root(path), data=data)
