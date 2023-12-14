import requests

query = """
select distinct subscriptionId
from mgw.subscriptions
where eventDateDate = '2023-07-27'
and eventType in ('CREATE', 'PROLONGATION')
"""

user = 'airflow_etl'
password = 'tgl7uJ+qJuOUJnCO'

resp = requests.post(url='http://ch-cluster-1.int.superanalytics.ru:38123/mgw', data=query, auth=(user, password))
ch_suscription_ids = resp.text.split('\n')
ch_suscription_ids = set([int(val) for val in ch_suscription_ids if val])


with open('subscription_27.csv', 'r') as f:
    f_suscription_ids = f.read()
f_suscription_ids = set([int(val) for val in f_suscription_ids[3:].split('\n') if val])

dif = [str(val) for val in list(f_suscription_ids.difference(ch_suscription_ids))]

with open('result_subscription_27.csv', 'w') as f:
    f.write('\n'.join(dif))

print(len(ch_suscription_ids), len(f_suscription_ids), len(dif))
