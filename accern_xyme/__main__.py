
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
        "user",
        type=str,
        help="the username")
    parser.add_argument(
        "password",
        type=str,
        help="the password")
    parser.add_argument(
        "--pipeline",
        type=str,
        help="the pipeline to pretty print")
    parser.add_argument(
        "--no-unicode",
        dest="no_uni",
        action="store_true",
        help="avoid unicode characters in the output")
    args = parser.parse_args()

    with accern_xyme.create_xyme_session(
            args.server, args.user, args.password) as xyme:
        pipe = xyme.get_pipeline(args.pipeline)
        print(pipe.pretty(not args.no_uni))


if __name__ == "__main__":
    run()
