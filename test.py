import os
import os.path

from rich.progress import Progress
from packaging.metadata.raw import parse_email


def test(print, filename):
    with open(filename, "rb") as fp:
        data_bytes = fp.read()

    # pkg_m = _from_pkg_metadata(data_bytes)
    raw, leftover = parse_email(data_bytes)

    # A lot of stuff has a license-file key, even though that PEP
    # isn't accepted yet.
    leftover.pop("license-file", None)

    if leftover:
        print(f"Leftover data parsing: {filename}:")
        for key, value in leftover.items():
            print(f"{key}: {value}")

        return False

    return True


all_metas = []
for root, dirs, files in os.walk("data/metadata"):
    for filename in files:
        if filename in {"PKG-INFO", "METADATA"}:
            all_metas.append(os.path.join(root, filename))

failed = []
with Progress() as progress:
    raw_success = 0
    raw_leftover = 0
    metas_t = progress.add_task("Testing metadata", total=len(all_metas))
    for filename in all_metas:
        raw = test(progress.console.print, filename)
        if raw:
            raw_success += 1
        else:
            raw_leftover += 1
            progress.console.print(f"Success: {raw_success} LeftOver: {raw_leftover}")
            failed.append(filename)
        progress.update(metas_t, advance=1)


with open("failed.txt", "w") as fp:
    fp.write("\n".join(failed) + "\n")
