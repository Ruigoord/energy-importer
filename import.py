import csv
import glob
from datetime import datetime
import decimal

import itertools

import requests

BUFFER_SIZE = 5000
URL = 'http://localhost:8086/write?db=ruigoord'


def sanitize_number(s):
    try:
        # If string contains a number sanitize it, eg.:
        # 00000000000002,1 -> 2.1
        return str(decimal.Decimal(s.replace(',', '.')))
    except decimal.InvalidOperation:
        return s


def line_reader():
    for filename in glob.iglob('**/*.CSV', recursive=True):
        location = filename.split('/')[0]

        with open(filename) as csvfile:
            csv_reader = csv.DictReader(csvfile, delimiter=';')

            for row in csv_reader:
                t = datetime.strptime(
                    f'{row["Date"]} {row["Time"]}', "%d/%m/%y %H:%M:%S"
                )
                timestamp = int(t.timestamp())

                # Remove the datetime info before sending all data as vars
                del row['Date']
                del row['Time']

                try:
                    value = sanitize_number(
                        row['Imp. Act. Energy S T1 kWh (3)']
                    )
                except AttributeError:
                    print('Invalid measurement:', row)

                yield f'{location} value={value} {timestamp}'


if __name__ == "__main__":
    line_reader = line_reader()

    try:
        with requests.Session() as session:
            while True:
                data = '\n'.join(itertools.islice(line_reader, BUFFER_SIZE))
                response = session.post(URL, data=data)
                assert response.status_code == 204, response.text
    except StopIteration:
        pass



