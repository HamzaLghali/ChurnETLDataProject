import pandas as pd
import datetime
from dbconnection import get_connection, get_cursor

# Connect to db
conn = get_connection()
cur = get_cursor(conn)

# Load CSV into DataFrame 
df = pd.read_csv('../../SSISFolder/churn data/campaign_desc.csv')

# Insert DataFrame rows to database
for index, row in df.iterrows():
    description = row['DESCRIPTION']
    campaign = row['CAMPAIGN']
    start_day = int(row['START_DAY'])
    end_day = int(row['END_DAY'])

    start_date = datetime.date(1900, 1, 1) + datetime.timedelta(days=start_day)
    end_date = datetime.date(1900, 1, 1) + datetime.timedelta(days=end_day)

    cur.execute("""
        INSERT INTO compaign_desc (DESCRIPTION, CAMPAIGN, START_DAY, END_DAY) 
        VALUES (%s, %s, %s, %s)
    """, (description, campaign, start_date, end_date))

conn.commit()
cur.close()
conn.close()