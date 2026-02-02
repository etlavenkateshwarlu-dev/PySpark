
count=1
with open('transaction.csv','r') as f:
    while True:
        record=f.readline()
        if not record:
            break
        print(record)


