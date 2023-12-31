
from datetime import datetime
import argparse
import sys
import os
from typing import List, NamedTuple, cast


class Args(NamedTuple):
    origin_path_or_url: str
    destin_path: str


def ger_cli_args(args: List[str]) -> Args:
    parser = argparse.ArgumentParser()
    parser.add_argument("--origin-path-or-url", required=True, type=str)
    parser.add_argument("--destin-path", required=True, type=str)
    return cast(Args, parser.parse_args(args))


# this method will download and save 
def download_and_save_version(url: str, destin_path: str) ->  None:
    file_extension = url.split(".")[-1]
    raw_content = open(url).read()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    os.makedirs(destin_path, exist_ok=True)

    with open(f"{destin_path}/{timestamp}.{file_extension}", "w") as f:
        f.write(raw_content)

    with open(f"{destin_path}/current.{file_extension}", "w") as f:
        f.write(raw_content)



if __name__ == "__main__":

    args = ger_cli_args(sys.argv[1:])
    origin_path_or_url = args.origin_path_or_url
    destin_path = args.destin_path
    
    download_and_save_version(
        url=origin_path_or_url,
        destin_path=destin_path,
    )

