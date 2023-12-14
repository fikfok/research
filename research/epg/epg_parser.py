import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Optional
import pandas as pd
import xmltodict
from pydantic import BaseModel, Field, Extra, validator, field_validator
from lxml import etree


class ParentModel(BaseModel):
    class Config:
        extra = 'ignore'
        populate_by_name = True

    def __init__(self, **kwargs):
        """
        Цель выражения ниже следующая.
        Желаемое поведение: если атрибуту присваивается None, ему необходимо присвоить default значение.
        None у атрибутов дочерних классов допустимы и прописано default значение. Но, если передать None в атрибут
        и с учётом default значения, то это default значение НЕ БУДЕТ присвоено атрибуту.
        Для этого можно прописать валидатор вида "@validator('*', pre=True)" и, если значение None,
        то возвращать default значение. Но предложенный ниже вариант короче.
        """
        _kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**_kwargs)


class ParentModel(BaseModel):
    class Config:
        extra = 'ignore'
        populate_by_name = True

    def __init__(self, **kwargs):
        """
        Цель выражения ниже следующая.
        Желаемое поведение: если атрибуту присваивается None, ему необходимо присвоить default значение.
        None у атрибутов дочерних классов допустимы и прописано default значение. Но, если передать None в атрибут
        и с учётом default значения, то это default значение НЕ БУДЕТ присвоено атрибуту.
        Для этого можно прописать валидатор вида "@validator('*', pre=True)" и, если значение None,
        то возвращать default значение. Но предложенный ниже вариант короче.
        """
        _kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**_kwargs)


class ChannelInSchema(ParentModel):
    name: str = Field(alias='Name')
    efir_week: str = Field(alias='EfirWeek')
    channel: str = Field(alias='Channel')
    channel_id: str = Field(alias='ChannelID')
    variant: str = Field(alias='Variant')
    dttm: str = Field(alias='DateTime')


class TitleInSchema(ParentModel):
    title: str = Field(alias='#text')


class SubTitleInSchema(ParentModel):
    sub_title: str = Field(alias='#text')


class DescInSchema(ParentModel):
    desc: str = Field(alias='#text')


class PersonInSchema(ParentModel):
    person: Optional[str] = Field(default=None, alias='#text')
    kinopoisk_id: Optional[int] = Field(default=None, alias='@kinopoisk_id')


class CreditsInSchema(ParentModel):
    director: Optional[Union[List[Union[PersonInSchema, str]], PersonInSchema, str]] = []
    presenter: Optional[Union[List[Union[PersonInSchema, str]], PersonInSchema, str]] = []
    writer: Optional[Union[List[Union[PersonInSchema, str]], PersonInSchema, str]] = []
    composer: Optional[Union[List[Union[PersonInSchema, str]], PersonInSchema, str]] = []
    editor: Optional[Union[List[Union[PersonInSchema, str]], PersonInSchema, str]] = []
    producer: Optional[Union[List[Union[PersonInSchema, str]], PersonInSchema, str]] = []

    @validator('*')
    def val(cls, raw_value):
        if isinstance(raw_value, list):
            for index, item in enumerate(raw_value):
                if isinstance(item, str):
                    raw_value[index] = PersonInSchema(**{'#text': item})
        elif isinstance(raw_value, str):
            raw_value = [PersonInSchema(**{'#text': raw_value})]
        elif isinstance(raw_value, PersonInSchema):
            raw_value = [raw_value]
        else:
            # Данная ветка вряд ли сработает, но пусть будет для надёжности
            raw_value = [PersonInSchema(**{'#text': str(raw_value)})]
        return raw_value


class EpisodeNumInSchema(ParentModel):
    episode_num: str = Field(alias='#text')


class CategoryInSchema(ParentModel):
    lang: str = Field(alias='@lang')
    category: Optional[str] = Field(default=None, alias='#text')


class KinopoiskInSchema(ParentModel):
    movie_id: Optional[str] = Field(default=None, alias='@movieid')
    rate: Optional[float] = Field(default=None, alias='@rate')


class ImdbInSchema(ParentModel):
    movie_id: Optional[str] = Field(default=None, alias='@movieid')
    imdb: Optional[str] = Field(default=None, alias='#text')


class IconInSchema(ParentModel):
    icon: str = Field(alias='@src')


class CountryInSchema(ParentModel):
    country: str = Field(alias='#text')


class RatingInSchema(ParentModel):
    rating: str = Field(alias='value')


class ProgrammeInSchema(ParentModel):
    """Входящая структура данных. В том виде, в котором она в xml файле"""

    start: Optional[str] = Field(default=None, alias='@start')
    stop: Optional[str] = Field(default=None, alias='@stop')
    channel: Optional[str] = Field(default=None, alias='@channel')
    id: Optional[int] = Field(default=None, alias='@id')
    original_id: Optional[int] = Field(default=None, alias='@original_id')
    lastmod: Optional[str] = Field(default=None, alias='@lastmod')
    title: Union[List[TitleInSchema], TitleInSchema] = []
    sub_title: Optional[SubTitleInSchema] = None
    serial_id: Optional[str] = None
    gate_id: Optional[str] = None
    desc: Optional[DescInSchema] = None
    credits: Optional[CreditsInSchema] = None
    company: Optional[str] = None
    episode_num: Optional[EpisodeNumInSchema] = Field(default=None, alias='episode-num')
    kinopoisk: Optional[KinopoiskInSchema] = None
    imdb: Optional[ImdbInSchema] = None
    category: Optional[Union[List[CategoryInSchema], CategoryInSchema]] = []
    date: Optional[str] = Field(default=None, alias='date')
    icon: Optional[Union[List[IconInSchema], IconInSchema]] = []
    year: Optional[str] = None
    country: Optional[CountryInSchema] = None
    rating: Optional[RatingInSchema] = None
    download_dt: str
    epg_id: str
    channel_dt: str

    @validator('icon', 'title')
    def val(cls, raw_value):
        if isinstance(raw_value, (IconInSchema, TitleInSchema)):
            raw_value = [raw_value]
        return raw_value


class ProgrammeOutSchema(ParentModel):
    """Исходящая структура данных. В том виде, в котором она отправляется в kafka"""
    # Символ разделитель ключей в итоговом словаре
    _SEP_SYMBOL = '_'

    mgw_id: str = ''
    id: int = None
    start: Optional[str] = None
    stop: Optional[str] = None
    channel: Optional[str] = None
    original_id: Optional[int] = 0
    lastmod: Optional[str] = None
    title: Optional[str] = None
    sub_title: Optional[str] = None
    serial_id: Optional[str] = None
    gate_id: Optional[str] = None
    desc: Optional[str] = None
    credits_director: Optional[str] = '[]'
    credits_presenter: Optional[str] = '[]'
    credits_writer: Optional[str] = '[]'
    credits_composer: Optional[str] = '[]'
    credits_editor: Optional[str] = '[]'
    credits_producer: Optional[str] = '[]'
    company: Optional[str] = None
    episode_num: Optional[str] = None
    kinopoisk: Optional[str] = None
    imdb: Optional[str] = None
    category: Optional[str] = '[]'
    date: Optional[str] = None
    icon: Optional[str] = '[]'
    year: Optional[str] = None
    country: Optional[str] = None
    rating: Optional[str] = None
    download_dt: str
    epg_id: str
    channel_dt: str

    def __init__(self, in_message: ProgrammeInSchema, **kwargs):
        """
        Парсинг входящего сообщения в выходящее
        """
        dumped_in_message = in_message.dict()
        flatten_in_message = pd.json_normalize(dumped_in_message, sep=self._SEP_SYMBOL).to_dict(orient='records')[0]
        flatten_in_message = {self._normalize_key(key): val for key, val in flatten_in_message.items()}

        # Список словарей со строками перевести в список строк для простоты структуру
        keys = ['category', 'icon', 'title']
        for key in keys:
            flatten_in_message[key] = [item[key] for item in flatten_in_message[key] if item[key]]

        if isinstance(flatten_in_message['title'], list):
            flatten_in_message['title'] = '. '.join(flatten_in_message['title'])

        # Оптимизация списка словарей с персонами. Бывают персоны со значением None в ключе kinopoisk_id, но с ФИО
        # в ключе person. Вот такие kinopoisk_id с None надо исключать.
        keys_for_optimize = [
            'credits_director',
            'credits_presenter',
            'credits_writer',
            'credits_composer',
            'credits_editor',
            'credits_producer',
        ]
        for key in keys_for_optimize:
            for index, item in enumerate(flatten_in_message.get(key, [])):
                flatten_in_message[key][index] = {k: v for k, v in item.items() if v}

        for key in flatten_in_message:
            if isinstance(flatten_in_message[key], (dict, list)):
                flatten_in_message[key] = json.dumps(flatten_in_message[key])

        flatten_in_message['mgw_id'] = f'{flatten_in_message["original_id"]}_{flatten_in_message["epg_id"]}'

        super().__init__(**flatten_in_message)

    def _normalize_key(self, key: str) -> str:
        """
        В силу особенностей модели в результирующем сыром словаре могут быть вложенные словари с дублирующим
        родительским ключом. Например: rating.rating.
        Цели функции: избавиться от дублирования в ключе
        """
        _key = key
        if self._SEP_SYMBOL in key:
            deduplicated_keys = []
            for k in key.split(self._SEP_SYMBOL):
                # Здесь принципиально важно сохранить порядок, потому set'ом воспользоваться нельзя
                if k not in deduplicated_keys:
                    deduplicated_keys.append(k)
            _key = self._SEP_SYMBOL.join(deduplicated_keys)
        return _key


with open('1688996232.xml', 'r', encoding='cp1251') as f:
    raw_epg = f.read()
# raw_epg = raw_channels.replace('&', '&amp;')
epg_data = xmltodict.parse(raw_epg)
parsed_epg = []
counter = 0
for item in epg_data['tv']['programme']:

    try:
        item['download_dt'] = '111'
        item['epg_id'] = '222'
        item['channel_dt'] = '333'
        programme_in = ProgrammeInSchema(**item)
        programme_out = ProgrammeOutSchema(programme_in)
    except Exception as e:
        a=1

a=1
