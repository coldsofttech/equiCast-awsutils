from equicast_awsutils.cost import CodeArtifact


def main():
    ca = CodeArtifact(folder="../dist")
    ca.calculate()


if __name__ == "__main__":
    main()
