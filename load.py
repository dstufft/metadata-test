# type: ignore
import functools

import aiometer
import trio
import httpx

from rich.progress import Progress
from packaging.utils import canonicalize_name

from data_pb2 import Project, Projects, File


AS_JSON = {"Accept": "application/vnd.pypi.simple.v1+json"}


async def fetch_file_list(client, project):
    resp = await client.get(f"https://pypi.org/simple/{project}/", headers=AS_JSON)
    resp.raise_for_status()

    data = resp.json()

    project = Project()
    project.name = data["name"]

    for fdata in data.get("files", []):
        file = File()
        file.filename = fdata["filename"]
        file.url = fdata["url"]
        file.sha256 = fdata["hashes"].get("sha256", "")
        file.blake2b = fdata["hashes"].get("blake2b", "")

        project.files.append(file)

    return project


async def main():
    transport = httpx.AsyncHTTPTransport(retries=15)
    async with httpx.AsyncClient(transport=transport) as client:
        resp = await client.get("https://pypi.org/simple/", headers=AS_JSON)
        resp.raise_for_status()

        names = [canonicalize_name(p["name"]) for p in resp.json()["projects"]]
        out = Projects()

        with Progress() as progress:
            fetch = progress.add_task("Fetching Files...", total=len(names))
            async with aiometer.amap(
                functools.partial(fetch_file_list, client),
                names,
                max_at_once=100,
            ) as results:
                async for result in results:
                    progress.update(fetch, advance=1)
                    out.projects.append(result)

        with open("data/files.p", "wb") as fp:
            fp.write(out.SerializeToString(deterministic=True))


trio.run(main)
