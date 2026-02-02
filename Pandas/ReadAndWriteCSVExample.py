import pandas as pd
data = {
    "transaction_id": [1001, 1002, 1003, 1004, 1005, 1006, 1006],
    "user_id":        ["U1", "U2", "U3", "U4", "U2", "U5", "U5"],
    "city":           ["Hyd", "Blr", "Hyd", "Del", "Blr", "Chn", "Chn"],
    "amount":         [250.5, 500.0, None, 1200.0, 700.0, None, None],
    "status":         ["SUCCESS", "FAILED", "SUCCESS", "SUCCESS", "FAILED", "SUCCESS", "SUCCESS"],
    "event_time":     ["2026-01-01 10:01:00", "2026-01-01 10:05:00",
                       "2026-01-01 10:10:00", "2026-01-01 11:00:00",
                       "2026-01-01 11:10:00", "2026-01-01 12:00:00",
                       "2026-01-01 12:00:00"]
}

#df=pd.DataFrame(data)
#df.to_csv('transaction.csv',index=False)

#df=pd.read_csv('transaction.csv')

#df.to_json('tansaction_details.json')

df1=pd.read_json('tansaction_details.json')
print(df1)

#print(df.describe())
#print(df)
