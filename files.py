# type: ignore
import functools
import os
import os.path
import zipfile
import tarfile
import io
import multiprocessing

import aiometer
import httpx
import trio
import trio_parallel

from rich.progress import Progress
from packaging.utils import canonicalize_name

from data_pb2 import Projects, Project


def extract(dirname, file_type, content):
    if file_type == "wheel":
        try:
            with zipfile.ZipFile(io.BytesIO(content), allowZip64=True) as fp:
                for info in fp.infolist():
                    if info.is_dir():
                        continue

                    info_dir = info.filename.split("/")[0]
                    if info_dir.endswith(".dist-info"):
                        dest = os.path.join(dirname, info.filename)
                        os.makedirs(os.path.dirname(dest), exist_ok=True)
                        with fp.open(info, "r") as mf:
                            with open(dest, "wb") as ifp:
                                ifp.write(mf.read())
        except Exception:
            return False
    elif file_type == "sdist":
        try:
            with tarfile.open(fileobj=io.BytesIO(content), mode="r:*") as fp:
                for member in fp.getmembers():
                    if not member.isfile():
                        continue

                    if os.path.basename(member.name) in {
                        "pyproject.toml",
                        "PKG-INFO",
                    }:
                        dest = os.path.join(dirname, member.name)
                        os.makedirs(os.path.dirname(dest), exist_ok=True)
                        mf = fp.extractfile(member)
                        if mf is not None:
                            with open(dest, "wb") as ifp:
                                ifp.write(mf.read())
        except Exception:
            return False
    else:
        return None

    return True


async def fetch_project(client, project: Project):
    base = os.path.join("data", "metadata", project.name[:2], project.name)
    os.makedirs(base, exist_ok=True)

    failed = []

    for file in project.files:
        # This won't catch 100% of sdists and wheels, but it'll get enough
        if not file.filename.endswith((".whl", ".tar.gz")):
            continue

        file_type = "wheel" if file.filename.endswith(".whl") else "sdist"

        dirname = os.path.join(base, file_type, file.filename)
        os.makedirs(dirname, exist_ok=True)

        if not os.path.isfile(os.path.join(dirname, ".fetched")):
            try:
                resp = await client.get(file.url, headers={"Accept": "identity"})
                resp.raise_for_status()
            except Exception:
                failed.append(file.filename)
                continue

            result = await trio_parallel.run_sync(
                extract, dirname, file_type, resp.content
            )
            if result is not None and not result:
                failed.append(file.filename)

            if result:
                with open(os.path.join(dirname, ".fetched"), "wb") as fp:
                    pass

    return failed


async def main():
    data = Projects()
    with open("data/files.p", "rb") as fp:
        data.ParseFromString(fp.read())

    total = sum(len(p.files) for p in data.projects)

    transport = httpx.AsyncHTTPTransport(retries=15)
    async with httpx.AsyncClient(transport=transport) as client:
        with Progress() as progress:
            fetch = progress.add_task("Fetching Metadata...", total=total)
            async with aiometer.amap(
                functools.partial(fetch_project, client),
                data.projects,
                max_at_once=100,
            ) as results:
                async for failed in results:
                    progress.update(fetch, advance=1)
                    for f in failed:
                        progress.console.print(f"Failed fetching {f}")


if __name__ == "__main__":
    trio.run(main)
