
def run() -> None:
    import argparse
    import accern_xyme

    parser = argparse.ArgumentParser(
        prog="accern_xyme", description="Accern XYME API")
    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version=f"accern_xyme version {accern_xyme.__version__}")
    parser.add_argument(
        "server",
        type=str,
        help="the server URL")
    parser.add_argument(
        "token",
        type=str,
        help="the server token")
    parser.add_argument(
        "--namespace",
        type=str,
        help="the namespace")
    parser.add_argument(
        "--dag",
        type=str,
        help="the dag to pretty print")
    parser.add_argument(
        "--no-unicode",
        dest="no_uni",
        action="store_true",
        help="avoid unicode characters in the output")
    args = parser.parse_args()

    xyme = accern_xyme.create_xyme_client(
        args.server, args.token, args.namespace)
    pipe = xyme.get_dag(args.dag)
    print(pipe.pretty(not args.no_uni))


if __name__ == "__main__":
    run()
