from datetime import datetime

import requests


select_dates_query = """
select event_date
from appmet_stats.events_all_main
where event_date >= '2023-09-19'
group by event_date
order by event_date
"""

user = 'airflow_etl'
password = 'tgl7uJ+qJuOUJnCO'
ch_url = 'http://ch-cluster-1.int.superanalytics.ru:38123/'

try:
    resp = requests.post(url=ch_url, data=select_dates_query, auth=(user, password))
except Exception as ex:
    print(ex)
else:
    dates = [dt for dt in resp.text.split('\n') if len(dt) == 10]
    print(dates)


for dt in dates:
    insert_query = f"""
    insert into appmet_stats.events_all_main_TMP
    select *
    from appmet_stats.events_all_main
    where (((((time + playtime_ms) / 1000) / 60) / 60) < 24) AND ((os_name, event_name, screen) != ('ios', 'screen_show', '/launch'))
    and event_date = '{dt}';
    """
    print(insert_query)
    try:
        resp = requests.post(url=ch_url, data=insert_query, auth=(user, password))
    except Exception as ex:
        print(f'Insert error: {ex}')
    else:
        print('Insert done')
        print('-----------------------------')
