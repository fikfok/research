import asyncio
import dataclasses
import re
from dataclasses import asdict

import httpx
import pandas as pd
from asyncio_cache import lru_cache
from bs4 import BeautifulSoup
from requests_html import AsyncHTMLSession

BASE_URL = "https://www.kinopoisk.ru"
DEBUG_MODE = True
YEARS = [2022] #[2021, 2022, 2023] #
GET_ONLY_SINGLE_PAGE = True
SLEEP_TIMEOUT = 1
RESULT_FILE = "/home/sadovenkoda/python_projects/research/research/kinopoisk/result.csv"


@dataclasses.dataclass
class Film:
    title: str | None = None
    film_link: str | None = None
    year: int | None = None
    season: str | None = None
    timing: str | None = None
    premiere_date: str | None = None
    platforms: str | None = None
    originals: str | None = None
    episodes_count: str | None = None
    genres: str | None = None
    studios: str | None = None
    directors: str | None = None
    producers: str | None = None
    screenwriters: str | None = None
    actors: str | None = None
    synopsis: str | None = None
    rating: str | None = None
    rating_count: str | None = None


cookies = {
    "_yasc": "4lbO86iIM5KXveFzDcN9X0IYFxxoBNxsIl3o7bk2Nnh35maXCazPIVtWMBrvaaI=",
    "_ym_d": "1676299865",
    "_ym_isad": "1",
    "_ym_uid": "1676275565477867783",
    "cmtchd": "MTY3NjI3NTU2NTI3Nw==",
    "coockoos": "4",
    "crookie": "3iojenE/XY7gltceBJChlTZMn1+DtAOrVynDvY6LHgdXTUngVONLmBaX/gW7qVBrOuObftX4oa5MPrp1QbdYzShCon4=",
    "gdpr": "0",
    "i": "0Mqm2SWayxVLCSlPiqCDMJgXYaOmjRtOxeeetvbcQeRS0DmYxTJ3gSEYOHAj6l2EYYEp+D0IRA9rpuZYjG7WDwQ5q5M=",
    "L": "ZSUGf1BUYHRHQAR8fFl0dH8AfXhAB1BzIhRSJi4uHC0kJg5wUBEoVAAreDAqH1onWABsBD4=.1676299429.15252.342097.7e497ff283de6f39551e6536c7dc426d",
    "location": "1",
    "mda_exp_enabled": "1",
    "mda2_beacon": "1676299430051",
    "mobile": "no",
    "PHPSESSID": "f93ec0a4c04cb52e2bcf84ef66fa9471",
    "spravka": "dD0xNjc2Mjc1NzY1O2k9ODIuMTM4LjQ5LjIwOTtEPTlBRTdDNUE1N0Q2NEMzREI4QzY4NzczQzgxMzMwREY1Q0JCODlEQUYzNzRCOTc0RDQ1RUFBM0VEODNBRjBFQzRCNzdGRTJCNDExRENFNEE5RTg3NEFBRTREQUFCOTMwRDEzNjIyNjJDMkUzQzt1PTE2NzYyNzU3NjU5MDgxMjUwMDc7aD03NTNhNmFlMDM2ZDk0OTRkOWVmZDc0MzFiYTg2NTNiNQ==",
    "sso_status": "sso.passport.yandex.ru:synchronized",
    "tc": "1",
    "user_country": "ru",
    "ya_sess_id": "3:1676299430.5.0.1676299429163:0TGKUg:1a.1.2:1|1755307763.0.2.3:1676299429|30:10213808.90858.ymBTlO2BdYxQbGndq6hHZDxWSqo",
    "yandex_gid": "213",
    "yandex_login": "sadovenkoda@dialog-regions.ru",
    "yandexuid": "7199637901657703134",
    "ymex": "1678867567.oyu.7199637901657703134",
    "yp": "1676361967.yu.7199637901657703134",
    "ys": "c_chck.3912208883#udn.cDpzYWRvdmVua29kYUBkaWFsb2ctcmVnaW9ucy5ydQ%3D%3D",
    "yuidss": "7199637901657703134",
    "_csrf": "LOHtu_DhvocUaz1-WiRdx-4Q",
    "_csrf_csrf_token": "7pD3G-WfPX4FxaKhC6F01pDu0-ld25NRroS-korlXtA",
    "desktop_session_key": "3dd1717a675414021e2f274de89c34339269006305dcfe2254501457a8e760a8a8f140abbf8074a71429c85d6ea124de0ee36f4a143f69c8bc58bad03df72ed9e86cb1baebfcecb6e7e58de1806169ed795e12704fb07549024d694582c934e8",
    "desktop_session_key.sig": "6fSh9FWiVKBulAyCfogmk6Yq0hk"
}


def _save_page(content: str, file_name: str):
    with open(f"/home/sadovenkoda/python_projects/research/research/kinopoisk/{file_name}.html", "w") as f:
        f.write(content)
    assert 1 == 0


async def get_list_series_page(http_client, year: int, page: int):
    if DEBUG_MODE:
        with open("/home/sadovenkoda/python_projects/research/research/kinopoisk/list_series.html") as f:
            content = f.read()
            html_page = BeautifulSoup(content, "html.parser")
    else:
        url = f"{BASE_URL}/lists/movies/year--{year}/?b=released&b=russian&b=series&ss_subscription=ANY&page={page}"
        response = await http_client.get(url, cookies=cookies)
        assert response.status_code == 200
        html_page = BeautifulSoup(response.text, "html.parser")
        await asyncio.sleep(SLEEP_TIMEOUT)
    return html_page


@lru_cache
async def get_film_page(http_client, film_link: str):
    """
    HTTPX не осуществляет рендер html страницы после получения. Страница с карточкой фильма принимает
    окончательный вид только после отработки JS скриптов. Например, кнопка просмотра онлайн появляется только после
    отработки JS скриптов.
    А вот полный рендер страницы осуществляется библиотекой requests_html и в итоге кнопка просмотра онлайн уже
    будет добавлена. Потому для скачивания страницы применяется именно она.
    """
    if DEBUG_MODE:
        with open("/home/sadovenkoda/python_projects/research/research/kinopoisk/film_page_2.html") as f:
            content = f.read()
            html_page = BeautifulSoup(content, "html.parser")
    else:
        url = f"{BASE_URL}{film_link}"
        with AsyncHTMLSession() as session:
            response = await session.get(url, cookies=cookies, timeout=5)
            assert response.status_code == 200
            html_page = BeautifulSoup(response.text, "html.parser")
            await asyncio.sleep(SLEEP_TIMEOUT)

        # response = await http_client.get(url, cookies=cookies)
        # assert response.status_code == 200
        # html_page = BeautifulSoup(response.text, "html.parser")
        # await asyncio.sleep(SLEEP_TIMEOUT)
    return html_page


@lru_cache
async def get_episodes_list_page(http_client, film_link: str):
    if DEBUG_MODE:
        with open("/home/sadovenkoda/python_projects/research/research/kinopoisk/list_episodes_1.html") as f:
            content = f.read()
            html_page = BeautifulSoup(content, "html.parser")
    else:
        film_id = film_link.strip("/").split("/")[1]
        url = f"{BASE_URL}/film/{film_id}/episodes/"
        response = await http_client.get(url, cookies=cookies)
        assert response.status_code == 200
        html_page = BeautifulSoup(response.text, "html.parser")
        await asyncio.sleep(SLEEP_TIMEOUT)
    return html_page


def is_page_empty(html_page: BeautifulSoup):
    no_content = html_page.find("h2", attrs={"class": re.compile(r"^styles_heading__")})
    result = no_content.text.strip() if no_content else ""
    return result.upper() == "Ничего не найдено".upper()


def get_film_cards(html_page: BeautifulSoup):
    return html_page.findAll("div", attrs={"class": re.compile(r"^styles_content__")})


def get_film_title(film_card: BeautifulSoup):
    title_el = film_card.find("span", attrs={"class": re.compile(r"^styles_mainTitle__")})
    return title_el.text.strip() if title_el else ""


def get_film_timing(film_page: BeautifulSoup):
    timing = "0"
    timing_caption_el = film_page.find("div", string="Время")
    timing_el = timing_caption_el.nextSibling if timing_caption_el else None
    if timing_el:
        timing_caption = timing_el.text.strip().lower()
        timing_re = re.search(r'^[0-9]+', timing_caption)
        timing = timing_re.group() if timing_re else timing_caption
    return timing


def get_about_film(film_page: BeautifulSoup, data_label: str):
    data = ""
    if data_caption_el := film_page.find("div", string=data_label):
        if data_wrapper_el := data_caption_el.nextSibling:
            data = ", ".join([el.text.strip() for el in data_wrapper_el.findAll("a")])
    return data


def get_film_platforms(film_page: BeautifulSoup):
    platforms = []

    if film_page.find("button", attrs={"class": re.compile(r"^kinopoisk-watch-online-button")}):
        platforms = ["Кинопоиск"]

    if watch_with_option_el := film_page.find("div", attrs={"class": re.compile(r"^styles_subscriptionText__")}):
        platforms += [watch_with_option_el.text.strip()]

    platforms_wrapper_el = film_page.find("div", attrs={"class": re.compile(r"^styles_watchingServices__")})
    if not platforms_wrapper_el:
        platforms_wrapper_el = film_page.find("div", attrs={"class": re.compile(r"^styles_watchingServicesOnline__")})

    if platforms_wrapper_el:
        platforms_el = platforms_wrapper_el.findAll("span", attrs={"class": re.compile(r"^styles_title__")})
        platforms += [pl.text.strip() for pl in platforms_el]

    return ", ".join(platforms)


def get_film_link(film_card: BeautifulSoup):
    link_el = film_card.find("a", attrs={"href": re.compile(r"^/series/\d*/")})
    link = link_el.get("href") if link_el else ""
    if not link:
        link_el = film_card.find("a", attrs={"href": re.compile(r"^/film/\d*/")})
        link = link_el.get("href") if link_el else ""
    return link


async def get_season_number(http_client, film_link: str, year: int):
    season_number = ""
    season_caption = "Сезон "
    episodes_list_page = await get_episodes_list_page(http_client=http_client, film_link=film_link)
    if link_el := episodes_list_page.find("a", attrs={"name": f"y{year}"}):
        if table_el := link_el.findParent("table"):
            if episode_el := table_el.find("h1", string=re.compile(rf"^{season_caption}")):
                season_number = episode_el.text.strip().replace(season_caption, "")
    return season_number


async def get_episodes_count(http_client, film_link: str, year: int):
    episodes_count = ""
    episodes_list_page = await get_episodes_list_page(http_client=http_client, film_link=film_link)
    if link_el := episodes_list_page.find("a", attrs={"name": f"y{year}"}):
        if table_el := link_el.findParent("table"):
            episodes_count = len(table_el.findAll("span", string=re.compile(r"^Эпизод")))
    return episodes_count


def get_synopsis(film_page: BeautifulSoup):
    synopsis = ""
    if synopsis_wrapper_el := film_page.find("div", attrs={"class": re.compile(r"^styles_filmSynopsis__")}):
        if synopsis_el := synopsis_wrapper_el.find("p"):
            synopsis = synopsis_el.text.strip()
    return synopsis


def get_rating(film_page: BeautifulSoup):
    rating = ""
    if rating_wrapper_el := film_page.find("div", attrs={"class": re.compile(r"styles_filmRating__")}):
        if rating_el := rating_wrapper_el.select_one("span.film-rating-value span"):
            rating = rating_el.text.strip().replace(".", ",")
    return rating


def get_rating_count(film_page: BeautifulSoup):
    rating_count = ""
    if rating_wrapper_el := film_page.find("div", attrs={"class": re.compile(r"styles_filmRating__")}):
        if count_wrapper_el := rating_wrapper_el.find("div", attrs={"class": re.compile(r"^styles_countBlock__")}):
            if rating_count_el := count_wrapper_el.find("span", attrs={"class": re.compile(r"^styles_count__")}):
                rating_count = rating_count_el.text.strip().replace(".", ",")
    return rating_count


async def parse_years(http_client, year: int):
    films = list()
    current_page = 1
    while True:
        list_series_page = await get_list_series_page(http_client=http_client, year=year, page=current_page)
        if is_page_empty(list_series_page):
            break

        film_cards = get_film_cards(html_page=list_series_page)
        if len(film_cards) == 0:
            break

        for film_card in film_cards:
            film_link = get_film_link(film_card)
            season = await get_season_number(http_client=http_client, film_link=film_link, year=year)
            film_page = await get_film_page(http_client=http_client, film_link=film_link)
            premiere_date = await get_premiere_date(http_client=http_client, film_link=film_link, year=year)
            episodes_count = await get_episodes_count(http_client=http_client, film_link=film_link, year=year)
            film = Film(
                title=get_film_title(film_card),
                film_link=f"{BASE_URL}{film_link}",
                year=year,
                season=season,
                timing=get_film_timing(film_page),
                premiere_date=premiere_date,
                platforms=get_film_platforms(film_page),
                originals=get_originals(film_page),
                episodes_count=episodes_count,
                genres=get_about_film(film_page, "Жанр"),
                studios=get_studios(film_page),
                directors=get_about_film(film_page, "Режиссер"),
                producers=get_about_film(film_page, "Продюсер"),
                screenwriters=get_about_film(film_page, "Сценарий"),
                actors=get_actors(film_page),
                synopsis=get_synopsis(film_page),
                rating=get_rating(film_page),
                rating_count=get_rating_count(film_page),
            )
            films.append(film)
        current_page += 1

        if GET_ONLY_SINGLE_PAGE:
            break
    return films


async def get_premiere_date(http_client, film_link: str, year: int):
    premiere_date = ""
    episodes_list_page = await get_episodes_list_page(http_client=http_client, film_link=film_link)
    if link_el := episodes_list_page.find("a", attrs={"name": f"y{year}"}):
        for row in link_el.findParent("table").findAll("tr"):
            if len(row.findAll("td")) >= 2:
                premiere_date = row.findAll("td")[1].text.strip()
    return premiere_date


def get_originals(film_page: BeautifulSoup):
    studios = get_studios(film_page)
    return "1" if studios else "0"


def get_studios(film_page: BeautifulSoup):
    studios_caption = ""
    if studios_el := film_page.find("div", string="Цифровой релиз"):
        if studios_data_el := studios_el.nextSibling:
            studios_caption = studios_data_el.text.strip()
            studios_caption = re.sub(r"[0-9]{1,2} [а-яА-Я]+ [0-9]{2,4}", "", studios_caption)
            studios_caption = ", ".join([txt for txt in studios_caption.strip(", ").split(", ") if txt])
    return studios_caption


def get_actors(film_page: BeautifulSoup):
    actors = ""
    if actors_wrapper_el := film_page.find("div", attrs={"class": re.compile(r"^styles_actors__")}):
        actors = [el.text.strip() for el in actors_wrapper_el.select("li a", attrs={"href": re.compile(r"/name/\d*/")})]
        actors = ", ".join(actors)
    return actors


async def get_films(http_client):
    all_films = list()
    for year in YEARS:
        year_films = await parse_years(http_client=http_client, year=year)
        if year_films:
            all_films += year_films
    return all_films


def dump_films(films: list):
    data = list()
    for film in films:
        data.append(asdict(film))
    df = pd.DataFrame(data)
    df.to_csv(RESULT_FILE)


async def main():
    http_session = httpx.AsyncClient()
    async with http_session as http_client:
        all_films = await get_films(http_client=http_client)
        dump_films(films=all_films)


if __name__ == "__main__":
    asyncio.run(main())
