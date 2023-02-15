import pandas as pd


messages_file_path = "/home/sadovenkoda/res.txt"
df = pd.read_csv(messages_file_path, header=None)
df1 = df.iloc[1::2, 0].str.replace('"text": "ообщение ', '').str.replace(' из 200000 Отправка в Mailer "', '').replace(' ', '').astype(int).sort_values().to_frame()
assert len(df1[df1.duplicated()].index) == 0
