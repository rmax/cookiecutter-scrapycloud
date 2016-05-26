#!/usr/bin/env python
# Copyright (c) Rolando Espinoza.
# See gh:rolando/cookiecutter-scrapycloud/LICENSE for details.
"""A script to dump items storead in a collection."""
import argparse
import json
import os

import hubstorage



def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('project', help="Project ID.")
    parser.add_argument('collection', help="Collection name.")
    parser.add_argument('--apikey', default=os.environ.get('SHUB_APIKEY'),
                        help="Scrapinghub API Key (default: %(default)s)")
    parser.add_argument('--filter', help="Query filter in JSON format.")

    args = parser.parse_args()
    if not args.apikey:
        parser.error("--apikey required")

    try:
        filter = json.loads(args.filter) if args.filter else {}
    except ValueError:
        parser.error("Failed to parse --filter")

    if not args.project.isdigit():
        parser.error("Invalid project identifier")

    hsc = hubstorage.HubstorageClient(args.apikey)
    hsp = hsc.get_project(args.project)
    col = hsp.collections.new_store(args.collection)

    # Fetch items directly in json format.
    for obj in col.iter_json(**filter):
        print(obj)


if __name__ == "__main__":
    main()
