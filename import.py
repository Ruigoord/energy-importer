import csv
import glob
from datetime import datetime
import decimal

import itertools

import requests
from slugify import slugify

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
            reader = csv.DictReader(csvfile, delimiter=';')

            for row in reader:
                t = datetime.strptime(
                    f'{row["Date"]} {row["Time"]}', "%d/%m/%y %H:%M:%S"
                )
                timestamp = int(t.timestamp())

                # Remove the datetime info before sending all data as vars
                del row['Date']
                del row['Time']

                try:
                    values = ','.join([
                        '{key}={value}'.format(
                            key=slugify(key),
                            value=sanitize_number(value))
                        for key, value in row.items()
                    ])
                except TypeError:
                    # Sometime cols have None as header, ignore those
                    pass

                yield f'{location},{values} {timestamp}'


if __name__ == "__main__":
    reader = line_reader()

    try:
        with requests.Session() as session:
            while True:
                # data = '\n'.join(itertools.islice(reader, BUFFER_SIZE))
                # print(data)
                response = session.post(URL, data='bauduinlaan,serial_number=14245I7G0074 value=1 1482765660')
                print(response.status_code, response.text)
                # print(response)
                # response = session.post(URL, data=data)
                # print(response)
                # assert response.status_code == 204
                exit()
    except StopIteration:
        pass



