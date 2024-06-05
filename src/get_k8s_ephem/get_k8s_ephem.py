#!/usr/bin/env python3
"""Carry out the Kubernetes query and reduction to determine ephemeral
storage by pod."""

import json
import subprocess
from typing import Any


class Executor:
    """Executor assumes that `kubectl` is in your path and your kubeconfig
    is set up for the cluster you want to query.
    """
    
    def __init__(self) -> None:
        self._results: list[dict[str,Any]] = []

    def query(self) -> None:
        """Run the query."""
        nodes = self._get_nodes()
        for node in nodes:
            stats = self._run_json(
                [
                    "kubectl",
                    "get",
                    "--raw",
                    f"/api/v1/nodes/{node}/proxy/stats/summary"
                ]
            )
            pods = stats["pods"]
            nodedata: dict[str, Any] = {}
            for pod in pods:
                p_id = f"{pod['podRef']['namespace']}/{pod['podRef']['name']}"
                if "ephemeral-storage" not in pod:
                    continue
                es = pod["ephemeral-storage"]
                if not nodedata:
                    nodedata["availableBytes"] = es["availableBytes"]
                    nodedata["capacityBytes"] = es["capacityBytes"]
                    nodedata["usage"] = []
                nodedata["usage"].append(
                    {
                        "pod": p_id,
                        "usedBytes": es["usedBytes"]
                    }
                )
            if nodedata:
                nodedata["node"] = node
                nodedata["totalUsage"] = sum(
                    [ x["usedBytes"] for x in nodedata["usage"] ] 
                )
                self._results.append(nodedata)

    def report(self) -> None:
        """Print the results."""
        print(json.dumps(self._results))

    def _run_json(self, args: list[str]) -> dict[str, Any]:
        result=subprocess.run(args, capture_output=True, check=True)
        output=result.stdout.decode()
        return json.loads(output)

    def _get_nodes(self) -> list[str]:
        result=subprocess.run(
            [
                "kubectl",
                "get",
                "nodes",
            ],
            capture_output=True,
            check=True
        )
        output_lines = result.stdout.decode().split("\n")[1:]
        return [ x.split()[0] for x in output_lines if x ]
                              
        
def main() -> None:
    exc = Executor()
    exc.query()
    exc.report()

if __name__ == "__main__":
    main()
