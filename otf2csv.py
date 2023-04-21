#! /usr/bin/env python3

import argparse
import logging as log
import csv
from typing import Dict, Any
import otf2


def file_name(handle: otf2.definitions.IoHandle) -> str:
    ''' Extract the correct name from `IoHandle`, which is either the file name
        or the handle name. '''
    try:
        return handle.file.name
    except AttributeError:
        return handle.name


def otf2_to_csv(tracefile: str, csvfile: str) -> None:
    ''' Open `tracefile` and write it as CSV in to `csvfile`. '''

    with otf2.reader.open(tracefile) as trace:

        with open(csvfile, "w") as outfile:
            writer = csv.writer(outfile)
            writer.writerow(["Timestamp", "EventType", "Region", "Rank", "Attributes"])

            for location, event in trace.events:
                # log.debug(f"location: {location} event: {event}")

                event_type = type(event).__name__
                region_name = event.region.name if hasattr(event, 'region') else ""

                attributes: Dict[str, Any] = {}
                if event.attributes:
                    attributes = {attr.name.lower(): value for attr, value in event.attributes.items()}

                if isinstance(event, otf2.events.IoCreateHandle):
                    attributes.update({
                        "mode": event.mode,
                        "creation_flags": event.creation_flags,
                        "status_flags": event.status_flags,
                        "handle": file_name(event.handle),
                        })

                if isinstance(event, otf2.events.IoDestroyHandle):
                    attributes.update({
                        "handle": file_name(event.handle),
                        })

                if isinstance(event, otf2.events.IoOperationBegin):
                    attributes.update({
                        "bytes_request": event.bytes_request,
                        "handle": file_name(event.handle),
                        })

                if isinstance(event, otf2.events.IoOperationComplete):
                    attributes.update({
                        "bytes_result": event.bytes_result,
                        "handle": file_name(event.handle),
                        })

                if isinstance(event, otf2.events.IoSeek):
                    attributes.update({
                        "offset_request": event.offset_request,
                        "offset_result": event.offset_result,
                        "whence": event.whence,
                        "handle": file_name(event.handle),
                        })

                writer.writerow([event.time, event_type, region_name, location.group.name, attributes])


def main() -> None:

    parser = argparse.ArgumentParser()
    parser.add_argument("tracefile", help="path to otf2 trace file", type=str)
    parser.add_argument("outfile", help="path to csv output file", type=str)
    parser.add_argument("--log", help="set the log level", type=str, choices=['INFO', 'DEBUG', 'WARNING', 'ERROR', 'CRITICAL'], default='INFO')

    args = parser.parse_args()
    log.basicConfig(level=args.log.upper())
    otf2_to_csv(args.tracefile, args.outfile)


if __name__ == "__main__":
    main()
