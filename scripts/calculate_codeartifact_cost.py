from pathlib import Path

from equicast_awsutils.cost import CodeArtifact


def main():
    folder_path = Path(__file__).parent.parent / "dist"
    ca = CodeArtifact(folder=folder_path)
    ca.calculate()


if __name__ == "__main__":
    main()
