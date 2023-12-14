from datetime import datetime

import requests


select_dates_query = """
select event_date
from devnull.kion_metrika
group by event_date
order by event_date
"""

user = 'airflow_etl'
password = 'tgl7uJ+qJuOUJnCO'

# try:
#     resp = requests.post(url='http://ch-cluster-1.int.superanalytics.ru:38123/', data=select_dates_query, auth=(user, password))
# except Exception as ex:
#     print(ex)
# else:
#     dates = [dt for dt in resp.text.split('\n') if len(dt) == 10]
#     with open(r"dates.txt", 'w') as fp:
#         for dt in dates:
#             fp.write(f'{dt}\n')

# 1979-12-31
# 2000-10-10
# 2001-03-16
# 2001-12-31
# 2002-01-01
# 2004-12-31

prev_dt = '1900-01-01'
rows_counter = 1
while True:
    with open(r"dates.txt", 'r') as fp:
        all_rows = fp.readlines()

    if rows_counter > len(all_rows):
        break
    rows_counter += 1

    row = all_rows[:rows_counter][-1]
    row = row.replace('\n', '')
    current_dt = row.split('\t')[0]
    if datetime.strptime(current_dt, '%Y-%m-%d') > datetime.strptime(prev_dt, '%Y-%m-%d'):
        prev_dt = current_dt
    else:
        continue

    if len(row.split('\t')) > 1:
        prev_result = row.split('\t')[1]
    else:
        prev_result = None

    if prev_result in ['SUCCESS']:
        continue

    if current_dt:
        print(f'{current_dt} ...')
        insert_query = f"""
            insert into devnull.kion_metrika_NEW
            select *
            from (
                 select *
                 from devnull.kion_metrika
                 where event_date = '{current_dt}'
            ) as t
        """

        print(insert_query)

        try:
            resp = requests.post(url='http://ch-cluster-1.int.superanalytics.ru:38123/', data=insert_query, auth=(user, password))
        except Exception as ex:
            print(ex)
            print(f'{current_dt} ERROR')
            print('-----------------------')
            result = 'ERROR'
        else:
            print(f'{current_dt} SUCCESS')
            result = 'SUCCESS'

        with open(r"dates.txt", 'r') as fp:
            rows = fp.readlines()
            rows = [rw.replace('\n', '') for rw in rows]

        with open(r"dates.txt", 'w') as fp:
            for rw in rows:
                if current_dt == rw.split('\t')[0]:
                    fp.write(f'{current_dt}\t{result}\n')
                else:
                    fp.write(f'{rw}\n')
    else:
        break
