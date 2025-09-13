import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from equicast_awsutils.cost import CodeArtifact


@pytest.fixture
def tmp_files(tmp_path):
    files = []
    for i in range(3):
        f = tmp_path / f"file_{i}.txt"
        f.write_text("a" * 1024 ** 2)  # 1 MB each
        files.append(f)
    return tmp_path, files


def mock_file(size_gb):
    mock_path = MagicMock(spec=Path)
    mock_path.is_file.return_value = True
    mock_path.stat.return_value = SimpleNamespace(st_size=size_gb * 1024 ** 3)
    return mock_path


def test_invalid_region(tmp_files):
    folder, _ = tmp_files
    ca = CodeArtifact(folder=folder, region="us-east-1")
    with pytest.raises(ValueError):
        ca.calculate()


def test_total_gb_calculation(tmp_files):
    folder, files = tmp_files
    ca = CodeArtifact(folder=folder)
    result = ca.calculate()
    expected_gb = 3 / 1024  # 3 MB in GB
    assert abs(result["total_gb"] - expected_gb) < 1e-6


def test_cost_and_color(tmp_files):
    folder, files = tmp_files
    ca = CodeArtifact(folder=folder, threshold=0.1)
    result = ca.calculate()
    assert result["color_code"] in ["green", "yellow", "red"]
    assert result["total_cost"] > 0


def test_github_output(tmp_files, tmp_path):
    folder, _ = tmp_files
    gh_output = tmp_path / "github_output.txt"
    gh_summary = tmp_path / "github_summary.md"

    os.environ["GITHUB_OUTPUT"] = str(gh_output)
    os.environ["GITHUB_STEP_SUMMARY"] = str(gh_summary)

    ca = CodeArtifact(folder=folder, github_output=True, github_summary=True, threshold=0.1)
    ca.calculate()

    with open(gh_output, "r", encoding="utf-8") as f:
        content = f.read()
        assert "total_gb=" in content
        assert "estimate_cost=" in content
        assert "color_code=" in content

    with open(gh_summary, "r", encoding="utf-8") as f:
        content = f.read()
        assert "### 💰 CodeArtifact Cost Estimate" in content
        assert "<span style='color:" in content

    del os.environ["GITHUB_OUTPUT"]
    del os.environ["GITHUB_STEP_SUMMARY"]


def test_files_list(tmp_files):
    _, files = tmp_files
    ca = CodeArtifact(files=files)
    result = ca.calculate()
    expected_gb = len(files) / 1024
    assert abs(result["total_gb"] - expected_gb) < 1e-6
