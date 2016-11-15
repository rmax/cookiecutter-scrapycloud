#!/usr/bin/env python
# Copyright (c) Rolando Espinoza.
# See gh:rolando/cookiecutter-scrapycloud/LICENSE for details.
"""An script to archive jobs items in a collection."""
import argparse
import contextlib
import datetime
import importlib
import logging
import os

import hubstorage


logging.getLogger('requests.packages.urllib3.connectionpool').setLevel(logging.WARNING)

logger = logging.getLogger('archive-items')


DEFAULT_COLNAME = 'archived_items'
PROCESSED_TAG = 'processed'
STATE_FINISHED = 'finished'


def get_default_project():
    jobkey = os.environ.get('SHUB_JOBKEY')
    if jobkey:
        project = jobkey.split('/', 1)[0]
        return project


def import_func(path):
    modname, _, funcname = path.rpartition('.')
    mod = importlib.import_module(modname)
    func = getattr(mod, funcname)
    if not callable(func):
        raise TypeError("function '%s' not callable" % path)
    return func


def process_job(job, process_func, write_func, batch_size, dry_run):
    n = 0
    for item in job.items.iter_values():
        if process_func:
            item = process_func(job, item)
            if item is None:
                continue

        if '_key' not in item:
            raise ValueError("item must have a _key field")

        if not dry_run:
            write_func(item)

        n += 1
        if n % batch_size == 0:
            logger.info("[%s] Stored %s items", job.key, n)

    return n


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('project', nargs='?', default=get_default_project(),
                        help="Project ID (default: current)")
    parser.add_argument('--apikey', default=os.environ.get('SHUB_APIKEY'),
                        help="Scrapinghub API key (default: %(default)s)")
    parser.add_argument('-t', '--has-tag', dest='tags', action='append', default=[],
                        help="Filter jobs by this tag.")
    parser.add_argument('--processed-tag', default=PROCESSED_TAG,
                        help="Tag to be applied to processed jobs (default: %(default)s)")
    parser.add_argument('--state', default=STATE_FINISHED,
                        help="Filter jobs by this state (default: %(default)s)")
    parser.add_argument('--writer-size', type=int, default=1000,
                        help="Collection write batch size (default: %(default)s)")
    parser.add_argument('-c', '--collection', default=DEFAULT_COLNAME,
                        help="Collection name where to store the items (default: %(default)s)")
    parser.add_argument('--process-func', help="Callable function to process items.")
    parser.add_argument('--limit', type=int, default=0,
                        help="Number of jobs to process (default: unlimited)")
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--debug', action='store_true')

    args = parser.parse_args()
    if not args.apikey:
        parser.error("--apikey required")

    if not args.project:
        parser.error("project required")

    if not args.project.isdigit():
        parser.error("invalid project value")

    try:
        process_func = import_func(args.process_func) if args.process_func else None
    except Exception as e:
        parser.error("--process-func wrong argument: %s" % e)

    level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(level=level)

    hsc = hubstorage.HubstorageClient(args.apikey)
    hsp = hsc.get_project(args.project)

    now = datetime.datetime.utcnow()
    col = hsp.collections.new_store(now.strftime(args.collection))
    writer = col.create_writer(size=args.writer_size)

    jobq_filter = {
        'state': args.state,
        'has_tag': args.tags,
        'lacks_tag': args.processed_tag,
    }
    if args.limit:
        jobq_filter['count'] = args.limit

    args_repr = repr(args).replace(args.apikey, '*****')
    logger.debug("Arguments: %s", args_repr)
    # Iterate over all jobs found a store items to our target collection.
    # Given item key is in job key namespace it is unique globally.
    total = 0
    with contextlib.closing(hsc):
        for obj in hsp.jobq.list(**jobq_filter):
            job = hsp.get_job(obj['key'])

            logger.info("[%s] Processing ...", job.key)
            n = process_job(job, process_func=process_func,
                            write_func=writer.write,
                            batch_size=args.writer_size,
                            dry_run=args.dry_run)
            logger.info("[%s] Stored %s items", job.key, n)
            total += n

            job.metadata['tags'].append(args.processed_tag)
            if not args.dry_run:
                job.metadata.save()

    logger.info("Stored %s items in total", total)


if __name__ == "__main__":
    main()
