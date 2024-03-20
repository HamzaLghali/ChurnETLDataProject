from dbconnection import get_connection, get_cursor, csvcon

# Connect to PostgreSQL database
conn = get_connection()
cur = get_cursor(conn)

# Open CSV file
with open('../../SSISFolder/churn data/campaign_desc.csv') as csv_file:
    csv_reader = csvcon.reader(csv_file)
    next(csv_reader)  # skip header

    # Insert data from CSV to database
    for row in csv_reader:
        description = row[0]
        campaign = row[1]
        start_day = row[2]
        end_day = row[3]

        cur.execute("""INSERT INTO campaign_desc 
                        (DESCRIPTION, CAMPAIGN, START_DAY, END_DAY)
                        VALUES (%s, %s, %s, %s)""",
                    (description, campaign, start_day, end_day))

conn.commit()
cur.close()
conn.close()
