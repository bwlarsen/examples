# Copyright 2023 MosaicML Streaming authors
# SPDX-License-Identifier: Apache-2.0

"""Convert the Pile dataset to streaming format.

    Instructions:

Download the Pile dataset (cf https://pile.eleuther.ai/) from the download site
(https://the-eye.eu/public/AI/pile/).

You then run this script specifying --in_root (the above dir), --out_root (the dir to create),
and any other flags as appropriate.
"""

import json
import os
from argparse import ArgumentParser, Namespace
from collections import Counter
from glob import glob
from multiprocessing import Pool
from typing import Dict, Iterator, List, Tuple

from streaming.base import MDSWriter
from streaming.base.util import get_list_arg


def parse_args() -> Namespace:
    """Parse command-line arguments.

    Args:
        Namespace: command-line arguments.
    """
    args = ArgumentParser()
    args.add_argument(
        '--out_root',
        type=str,
        required=True,
        help='Directory path to store the output dataset',
    )
    args.add_argument(
        '--compression',
        type=str,
        default='zstd:16',
        help='Compression algorithm to use. Empirically, Zstandard has the best performance in ' +
        'our benchmarks. Tune the compresion level (from 1 to 22) to trade off time for ' +
        'quality. Default: zstd:16',
    )
    args.add_argument(
        '--hashes',
        type=str,
        default='sha1,xxh64',
        help='Hashing algorithms to apply to shard files. Default: sha1,xxh64',
    )
    args.add_argument(
        '--size_limit',
        type=int,
        default=1 << 27,
        help='Shard size limit, after which point to start a new shard. Default: 1 << 27',
    )
    return args.parse_args()


def merge_shard_groups(root: str) -> None:
    """Merge ephemeral sub-datasets created in parallel into one dataset.

    Args:
        root (str): Root directory.
    """
    pattern = os.path.join(root, '*')
    subdirs = sorted(glob(pattern))
    print(subdirs)
    # shard_id = 0
    # infos = []
    # for subdir in subdirs:
    #     index_filename = os.path.join(subdir, 'index.json')
    #     obj = json.load(open(index_filename))
    #     for info in obj['shards']:
    #         old_basename = info['raw_data']['basename']
    #         new_basename = with_id(old_basename, shard_id)
    #         info['raw_data']['basename'] = new_basename

    #         old_basename = info['zip_data']['basename']
    #         new_basename = with_id(old_basename, shard_id)
    #         info['zip_data']['basename'] = new_basename

    #         old_filename = os.path.join(subdir, old_basename)
    #         new_filename = os.path.join(root, new_basename)
    #         assert not os.rename(old_filename, new_filename)

    #         shard_id += 1
    #         infos.append(info)

    #     assert not os.remove(index_filename)
    #     assert not os.rmdir(subdir)

    # index_filename = os.path.join(root, 'index.json')
    # obj = {
    #     'version': 2,
    #     'shards': infos,
    # }
    # text = json.dumps(obj, sort_keys=True)
    # with open(index_filename, 'w') as out:
    #     out.write(text)


def main(args: Namespace) -> None:
    """Convert the Pile to streaming format.

    Args:
        args (Namespace): Command-line arguments.
    """

    # Merge shard groups.
    train_root = os.path.join(args.out_root)
    merge_shard_groups(train_root)


if __name__ == '__main__':
    main(parse_args())