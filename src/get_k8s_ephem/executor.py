"""Carry out the Kubernetes query and reduction to determine ephemeral
storage by pod and images pulled to each node.
"""

import json
import subprocess
from pathlib import Path
from typing import Any


class Executor:
    """Executor assumes that `kubectl` is in your path and your kubeconfig
    is set up for the cluster you want to query.
    """

    def __init__(self, file: Path | None = None) -> None:
        self._results: list[dict[str, Any]] = []
        self._file = file

    def query(self) -> None:
        """Run the query."""
        nodes = self._get_nodes()
        for node in nodes:
            stats = self._run_json(
                [
                    "kubectl",
                    "get",
                    "--raw",
                    f"/api/v1/nodes/{node}/proxy/stats/summary",
                ]
            )
            nodejson = self._run_json(
                ["kubectl", "get", "node", node, "-o", "json"]
            )
            images = nodejson["status"]["images"]
            images.sort(key=lambda x: x["sizeBytes"], reverse=True)
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
                    nodedata["podUsage"] = []
                    nodedata["images"] = images
                nodedata["podUsage"].append(
                    {"pod": p_id, "usedBytes": es["usedBytes"]}
                )
            if nodedata:
                nodedata["podUsage"].sort(
                    key=lambda x: x["usedBytes"], reverse=True
                )
                nodedata["node"] = node
                nodedata["totalPodUsage"] = sum(
                    [x["usedBytes"] for x in nodedata["podUsage"]]
                )
                self._results.append(nodedata)

    def report(self) -> None:
        """Print the results."""
        res_str = json.dumps(self._results)
        if self._file:
            self._file.write_text(res_str)
        else:
            print(res_str)  # noqa: T201

    def _run_json(self, args: list[str]) -> dict[str, Any]:
        result = subprocess.run(args, capture_output=True, check=True)
        output = result.stdout.decode()
        return json.loads(output)

    def _get_nodes(self) -> list[str]:
        result = subprocess.run(
            [
                "kubectl",
                "get",
                "nodes",
            ],
            capture_output=True,
            check=True,
        )
        output_lines = result.stdout.decode().split("\n")[1:]
        return [x.split()[0] for x in output_lines if x]
