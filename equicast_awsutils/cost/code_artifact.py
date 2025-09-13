import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Union, List


@dataclass
class CodeArtifact:
    region: str = "eu-west-1"
    folder: Optional[Union[str, Path]] = field(default=None)
    files: Optional[List[Union[str, Path]]] = field(default=None)
    threshold: Optional[float] = field(default=None)
    github_summary: bool = field(default=False)
    github_output: bool = field(default=False)
    total_gb: float = field(default=0.0, init=False)
    total_cost: float = field(default=0.0, init=False)
    color_code: Optional[str] = field(default=None, init=False)
    all_files: List[Path] = field(default_factory=list, init=False)

    _pricing_per_gb = 0.05

    def _validate_region(self):
        if self.region != "eu-west-1":
            raise ValueError("Currently only Ireland (eu-west-1) region is supported")

        print(f"Using region: {self.region}")

    def _gather_files(self):
        all_files = []
        if self.folder:
            folder_path = Path(self.folder)
            if not folder_path.exists():
                raise FileNotFoundError(f"Folder {self.folder} does not exist")

            all_files.extend(folder_path.rglob("*"))
            print(f"Found {len(all_files)} files in folder {self.folder}")

        if self.files:
            all_files.extend(self.files)
            print(f"Added {len(self.files)} files from explicit list")

        self.all_files = [f for f in all_files if f.is_file()]

    def _calculate_total_gb(self):
        self.total_gb = sum(f.stat().st_size / (1024 ** 3) for f in self.all_files)
        print(f"Total size: {self.total_gb:.6f} GB")

    def _calculate_cost(self):
        self.total_cost = self.total_gb * self._pricing_per_gb
        print(f"Total cost for CodeArtifact: ${self.total_cost:.6f}")

    def _determine_color_code(self):
        if self.threshold is None:
            self.color_code = None
            return
        if self.total_cost < self.threshold * 0.5:
            color = "green"
        elif self.total_cost < self.threshold:
            color = "yellow"
        else:
            color = "red"

        self.color_code = color
        print(f"Color code based on threshold {self.threshold}: {self.color_code}")

    def _write_github_output(self):
        if self.github_output:
            gh_output = os.environ.get("GITHUB_OUTPUT")
            if gh_output:
                with open(gh_output, "a", encoding="utf-8") as f:
                    f.write(f"total_gb={self.total_gb:.6f}\n")
                    f.write(f"estimate_cost={self.total_cost:.6f}\n")
                    f.write(f"color_code={self.color_code}\n")

    def _write_github_summary(self):
        if self.github_summary:
            summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
            if summary_path:
                with open(summary_path, "a", encoding="utf-8") as f:
                    f.write("### 💰 CodeArtifact Cost Estimate\n")
                    f.write("| Resource | Storage Size (GB) | Estimated Monthly Cost |\n")
                    f.write("|----------|-------------------|------------------------|\n")
                    f.write(
                        f"| CodeArtifact | {self.total_gb:.6f} | "
                        f"<span style='color:{self.color_code}'>${self.total_cost:.6f}</span> |\n"
                    )

    def calculate(self) -> dict:
        self._validate_region()
        self._gather_files()
        self._calculate_total_gb()
        self._calculate_cost()
        self._determine_color_code()
        self._write_github_output()
        self._write_github_summary()

        return {
            "total_gb": self.total_gb,
            "total_cost": self.total_cost,
            "color_code": self.color_code
        }
