import pandas as pd


shipments = [
    '/home/fikfok/python_projects/research/research/data/xls/Поставка Казань 11.05.23.xlsx',
    '/home/fikfok/python_projects/research/research/data/xls/Поставка Казань 24.03.23.xlsx',
]


stocks = [
    '/home/fikfok/python_projects/research/research/data/xls/Отчет по остаткам 23.05.23.xlsx',
]


stck_df = pd.read_excel(stocks[0], engine='openpyxl')
stck_df = stck_df.drop(['Бренд', 'Товары в пути до клиента', 'Товары в пути от клиента', 'Итого по складам'], axis=1)
stck_df.fillna(0, inplace=True)
numeric_columns = set(stck_df.columns) - {'Предмет', 'Артикул продавца'}
stck_df = stck_df.astype({col: 'int' for col in numeric_columns})

stck_df = pd.melt(stck_df, id_vars=['Артикул продавца'], value_vars=list(numeric_columns), var_name='Склад', value_name='Остаток 23.05')
stck_df = stck_df[stck_df['Склад'] == 'Казань']
stck_df = stck_df.drop(['Склад'], axis=1)
stck_df = stck_df.set_index('Артикул продавца')

shp_df11 = pd.read_excel(shipments[0], engine='openpyxl')
shp_df11 = shp_df11.groupby(['Артикул поставщика'])[['Количество, шт.']].sum()
shp_df11 = shp_df11.rename(columns={'Количество, шт.': 'Поставки 11.05'})

shp_df24 = pd.read_excel(shipments[1], engine='openpyxl')
shp_df24 = shp_df24.groupby(['Артикул поставщика'])[['Количество, шт.']].sum()
shp_df24 = shp_df24.rename(columns={'Количество, шт.': 'Поставки 24.03'})


df = shp_df24.join(shp_df11)
df = df.join(stck_df)
df.fillna(0, inplace=True)
df = df.astype({col: 'int' for col in df.columns})
df['Процент выкупа'] = ((df['Поставки 11.05'] + df['Поставки 24.03']) - df['Остаток 23.05']) / (df['Поставки 11.05'] + df['Поставки 24.03']) * 100
df['Процент выкупа'] = df['Процент выкупа'].round(decimals=0).astype(int).astype(str) + ' %'
df = df.reset_index()
a=1