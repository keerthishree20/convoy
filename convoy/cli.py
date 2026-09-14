"""convoy: run a node, a local cluster, or talk to one.

    convoy up --size 3                          three local nodes, Ctrl-C to stop
    convoy serve --id n1 --cluster n1=127.0.0.1:7001:8001,n2=...,n3=... --data ./data/n1
    convoy put greeting hello --nodes 127.0.0.1:8001,127.0.0.1:8003
    convoy get greeting --nodes ...
    convoy status --nodes ...
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import sys
import time


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="convoy", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    serve = sub.add_parser("serve", help="run one node")
    serve.add_argument("--id", required=True)
    serve.add_argument("--cluster", required=True, help="id=host:raft_port:http_port,...")
    serve.add_argument("--data", required=True)
    serve.add_argument("--tick-ms", type=float, default=10)
    serve.add_argument("--no-fsync", action="store_true", help="unsafe; for measuring what fsync costs")

    up = sub.add_parser("up", help="run a local cluster in the foreground")
    up.add_argument("--size", type=int, default=3)
    up.add_argument("--data", default="./convoy-data")
    up.add_argument("--base-port", type=int, default=7001)

    for name in ("put", "get", "delete", "status"):
        p = sub.add_parser(name)
        if name != "status":
            p.add_argument("key")
        if name == "put":
            p.add_argument("value")
        p.add_argument("--nodes", default="127.0.0.1:7002,127.0.0.1:7004,127.0.0.1:7006",
                       help="HTTP addresses; the default matches `convoy up`")

    args = parser.parse_args(argv)

    if args.command == "serve":
        from .server import parse_cluster, serve as run

        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
        try:
            asyncio.run(run(args.id, parse_cluster(args.cluster), args.data, tick_ms=args.tick_ms, fsync=not args.no_fsync))
        except KeyboardInterrupt:
            pass
        return 0

    if args.command == "up":
        from .local import LocalCluster

        # SIGTERM takes the same path as Ctrl-C, so the nodes are never orphaned.
        signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
        cluster = LocalCluster(args.size, args.data, base_port=args.base_port)
        cluster.start()
        print("nodes:", ", ".join(f"{p.id} http://{p.http_address}" for p in cluster.peers.values()))
        print(f"logs in {cluster.log_dir}/. Ctrl-C stops the cluster.")
        try:
            leader = cluster.wait_for_leader()
            print(f"leader: {leader}")
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:
            cluster.stop()
        return 0

    from .client import ConvoyClient

    client = ConvoyClient(args.nodes.split(","), give_up_after=10)
    try:
        if args.command == "put":
            print(json.dumps(client.put(args.key, args.value)))
        elif args.command == "get":
            print(json.dumps(client.get(args.key)))
        elif args.command == "delete":
            print(json.dumps(client.delete(args.key)))
        else:
            for address in client.addresses:
                print(address, json.dumps(client.status(address)))
    finally:
        client.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
