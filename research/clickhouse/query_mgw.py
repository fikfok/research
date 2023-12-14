import time

import requests


query = """
select * from programs_stream limit 100;
"""

user = 'airflow_etl'
password = 'tgl7uJ+qJuOUJnCO'

while True:
    try:
        resp = requests.post(url='http://ch-cluster-1.int.superanalytics.ru:38123/mgw', data=query, auth=(user, password))
    except Exception as ex:
        print(ex)
    else:
        programs_stream = resp.text.split('\n')
        print(programs_stream[0])

    time.sleep(10)



# ("Connection broken: ConnectionResetError(104, 'Connection reset by peer')", ConnectionResetError(104, 'Connection reset by peer'))