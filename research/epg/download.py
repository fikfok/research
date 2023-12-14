from lxml import etree

import requests
from requests.cookies import RequestsCookieJar


CHANNELS_URL = 'https://xmltv.s-tv.ru/xchenel.php?login=tv9274&pass=FtnjHflhtx&xmltv=1&nottrim&new&phase2&show=2'


def get_channels_urls():
    channels_urls = []
    cookies = None
    try:
        response = requests.get(CHANNELS_URL)
    except Exception as e:
        msg = f'HTTP request error: {str(e)}'
        print(msg)
    else:
        if response.status_code == 200:
            # Здесь используется lxml, т.к. у него нет проблем при считывании URL, в который есть знак &
            # Можно было бы и xmltodict здесь использовать, но это надо заменять & на безопасное сочетание символов,
            # что тоже не очень хорошо.
            xml_parser = etree.XMLParser(ns_clean=True, recover=True, encoding="windows-1251")
            parsed_channels = etree.fromstring(response.content, parser=xml_parser)
            files = list(parsed_channels.xpath("File"))
            for file_node in files:
                url = file_node.xpath("Name")[0].text
                channels_urls.append(url)
            cookies = response.cookies
        else:
            print(f'HTTP request error. Status code "{str(response.status_code)}": {str(response.text)}')

    return cookies


def download_single_channel_epg(url: str, cookies: RequestsCookieJar) -> bytes:
    """
    Запросить xml файл с расписанием программ по каналу
    """
    try:
        response = requests.get(url, cookies=cookies)
    except Exception as e:
        msg = f'HTTP request error: {str(e)}'
        print(msg)
    else:
        if response.status_code == 200:
            if 'Неверный ID Сессии' in str(response.content):
                msg = f'HTTP request error: {str(response.content)}'
                print(msg)
        else:
            msg = f'HTTP request error "{response.status_code}": {str(response.content)}'
            print(msg)
    return response.content


cookies = get_channels_urls()
urls = [
    # 'http://xmltv.s-tv.ru/getpr.php?prg=1689675686&sh=0&phase2&pers=epg13_2',
    # 'http://xmltv.s-tv.ru/getpr.php?prg=1689835663&sh=0&phase2&pers=epg266_2',
    # 'http://xmltv.s-tv.ru/getpr.php?prg=1689914001&sh=0&phase2&pers=epg266_7',

    'http://xmltv.s-tv.ru/xmltv.php?prg=1689345356&sh=0&nottrim&new&phase2&pers=epg21domash',
    'http://xmltv.s-tv.ru/xmltv.php?prg=1689281365&sh=0&nottrim&new&phase2&pers=epg80',
    'http://xmltv.s-tv.ru/xmltv.php?prg=1688736922&sh=0&nottrim&new&phase2&pers=epg380',
]

for url in urls:
    res = download_single_channel_epg(url=url, cookies=cookies)
