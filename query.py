import csv

import requests


sensors = ['kerk', 'straat', 'msr-bauduinlaan', 'msr-t906']
query = 'SELECT difference(mean("value")) FROM "{sensor}" WHERE time >= 1479276010011ms and time <= 1520563055372ms GROUP BY time(1d) fill(null)'

with requests.Session() as session:
    params = {
        'pretty': True,
        'db': 'ruigoord',
        'q': ';'.join([query.format(sensor=sensor) for sensor in sensors])
    }
    response = session.get('http://localhost:8086/query', params=params)
    for result in response.json()['results']:
        data = result['series'][0]

        sensor_name = data['name']
        with open(f'export/{sensor_name}.csv', 'w') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(data['columns'])
            # Filter out the time part of the datetime since we aggregate
            # per day
            csv_writer.writerows(
                map(lambda x: [x[0][:10], x[1]], data['values'])
            )
