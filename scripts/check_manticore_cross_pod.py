"""Run on the named client Pod; a server-local request cannot satisfy this check."""
import argparse
import socket
from pathlib import Path
import httpx
import psutil

from openalex_api.config import atomic_json, config
from openalex_api.earth_control import read_json
from openalex_api.earth_delivery import check

parser = argparse.ArgumentParser()
parser.add_argument("--cases", required=True)
parser.add_argument("--output", required=True)
parser.add_argument("--base-url", default="http://127.0.0.1:8088")
args = parser.parse_args()
addresses = {a.address for values in psutil.net_if_addrs().values() for a in values if a.family == socket.AF_INET}
if "CLIENT_IP" not in addresses:
    raise SystemExit("Run this command in client Pod CLIENT_IP; this host is not that client.")
fixture = read_json(Path(args.cases))
with httpx.Client(trust_env=False, timeout=20, headers={"X-API-Key": config()["api_keys"][0]}) as client:
    response = client.get(args.base_url + "/status")
    response.raise_for_status()
    if response.json().get("search_build") != fixture["search_build"]:
        raise SystemExit("Server is not serving the fixture's search build; use its current qualification/cases.json.")
report = check(args.cases, args.output, args.base_url, "all")
report.update(cross_pod_tested=True, client_ip="CLIENT_IP", search_build=fixture["search_build"])
atomic_json(Path(args.output) / "acceptance.json", report)
print({"passed": report["passed"], "cross_pod_tested": True, "output": args.output})
raise SystemExit(0 if report["passed"] else 1)
