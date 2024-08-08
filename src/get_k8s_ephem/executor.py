"""Carry out the Kubernetes query and reduction to determine ephemeral
storage by pod and images pulled to each node.
"""

import json
import subprocess
from pathlib import Path
from typing import Any

IMAGESIZER_CTR = "docker.io/lsstsqre/imagesizer"
IMAGESIZER_VER = "0.0.1"


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
            image_text = self._run_text(
                [
                    "kubectl",
                    "debug",
                    "-it",
                    f"node/{node}",
                    "--image",
                    f"{IMAGESIZER_CTR}:{IMAGESIZER_VER}",
                    "--",
                    "/bin/crictl",
                    "-r",
                    "unix:///host/run/containerd/containerd.sock",
                    "images",
                ]
            )
            images = self._parse_images(image_text)
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
                    nodedata["imageCount"] = len(images)
                    nodedata["totalImageSize"] = sum(
                        [x["sizeBytes"] for x in images]
                    )

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

    def _parse_images(self, image_text: str) -> list[dict[str, Any]]:
        i_lines = image_text.split("\n")
        i_lines = i_lines[2:]  # Discard pod-starting message and header
        images = []
        for li in i_lines:
            if not li:
                continue
            (imagename, tag, imageid, size) = li.split()
            if tag == "<none>":
                tag = "latest"  # Docker default
            fullname = f"{imagename}:{tag}"
            i_size = self._parse_size(size)
            images.append(
                {"sizeBytes": i_size, "name": fullname, "id": imageid}
            )
        return images

    def _parse_size(self, size_text: str) -> int:
        if size_text.endswith("GB"):
            sz_f = float(size_text[:-2])
            return int(1e9 * sz_f)
        elif size_text.endswith("MB"):
            sz_f = float(size_text[:-2])
            return int(1e6 * sz_f)
        elif size_text.endswith("kB"):
            sz_f = float(size_text[:-2])
            return int(1e3 * sz_f)
        return 0  # Smaller than a KB, or unparseable == "Doesn't count"

    def report(self) -> None:
        """Print the results."""
        res_str = json.dumps(self._results)
        if self._file:
            self._file.write_text(res_str)
        else:
            print(res_str)  # noqa: T201

    def _run_text(self, args: list[str]) -> str:
        result = subprocess.run(args, capture_output=True, check=True)
        return result.stdout.decode()

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
