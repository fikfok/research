import json

import pandas as pd


# messages_file_path = "/home/sadovenkoda/Desktop/result 30000.json"
# messages_file_path = "/home/sadovenkoda/Desktop/result.json"
# messages_file_path = "/home/sadovenkoda/Downloads/Telegram Desktop/ChatExport_2023-02-06 (1)/result.json"
messages_file_path = "/home/sadovenkoda/Downloads/Telegram Desktop/ChatExport_2023-02-13/result.json"

file_handler = open(messages_file_path)
data = json.load(file_handler)
df = pd.DataFrame(data["messages"])
df1 = df[df["text"].str.endswith("Отправка в Mailer 1234567890.")]
# df1 = df[df["text"].str.startswith("40001 qwe Тестовое сообщение")]
# df1 = df[df["id"] >= 67694]
df1["date_unixtime"] = df1["date_unixtime"].astype(int)
df1["delta_minutes"] = ((df1["date_unixtime"] - min(df1["date_unixtime"])) / 60).round()
group_df = df1["delta_minutes"].value_counts().to_frame().reset_index()
group_df.to_csv("/home/sadovenkoda/Desktop/group 30000.csv")


