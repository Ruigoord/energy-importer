import os
import argparse
import csv
import glob
from datetime import datetime
import decimal

import itertools

import requests
from more_itertools import peekable
from progress.bar import IncrementalBar

BUFFER_SIZE = 5000
URL = 'http://localhost:8086/write?db=ruigoord'


def sanitize_number(s):
    try:
        # If string contains a number sanitize it, eg.:
        # 00000000000002,1 -> 2.1
        return str(decimal.Decimal(s.replace(',', '.')))
    except decimal.InvalidOperation:
        return s


def get_total_lines(paths):
    total_lines = 0
    for path in paths:
        with open(path) as f:
            total_lines += sum(1 for l in f)

    return total_lines


def line_reader(paths):
    for path in paths:
        # Location of sensor is first part of path
        location = path.split('/')[0]

        with open(path) as csvfile:
            csv_reader = csv.DictReader(csvfile, delimiter=';')

            for row in csv_reader:
                try:
                    t = datetime.strptime(
                        f'{row["Date"]} {row["Time"]}', "%d/%m/%y %H:%M:%S"
                    )
                except ValueError:
                    # On invalid time values skip to next row
                    # print('Ivalid time:', row['Date'], row['Time'])
                    continue

                timestamp = int(t.timestamp())

                # Remove the datetime info before sending all data as vars
                del row['Date']
                del row['Time']

                try:
                    value = sanitize_number(
                        row['Imp. Act. Energy S T1 kWh (3)']
                    )
                except AttributeError:
                    # On invalid value skip to next row
                    # print('Invalid measurement:', row)
                    continue

                yield f'{location} value={value} {timestamp}000000000'


def buffered_line_reader(lines, buffer_size):
    while True:
        chunk = list(itertools.islice(lines, buffer_size))
        if not chunk:
            return
        yield '\n'.join(chunk)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'path', help='Path to directory containing measurement data'
    )
    args = parser.parse_args()

    # Use given path as working directory for script so glob works from there
    os.chdir(args.path)

    total_lines = get_total_lines(glob.iglob('**/*.CSV', recursive=True))
    total_size = int(total_lines / BUFFER_SIZE)

    bar = IncrementalBar(
        max=total_size, suffix='%(percent)d%% [ETA: %(eta_td)s]'
    )
    bar.start()

    csv_glob = peekable(glob.iglob('**/*.CSV', recursive=True))
    line_reader = buffered_line_reader(
        line_reader(csv_glob), buffer_size=BUFFER_SIZE
    )

    with requests.Session() as session:
        # Iterate through the progress bar to auto update the bar
        for lines in bar.iter(line_reader):
            bar.message = csv_glob.peek(bar.message)
            response = session.post(URL, data=lines)
            assert response.status_code == 204, response.text
