from dbconnection import get_connection, get_cursor, csvcon

# Connect to PostgreSQL database
conn = get_connection()
cur=get_cursor(conn)

# Open CSV file
with open('../../../SSISFolder/churn data/hh_demographic.csv') as f:
    csv_reader = csvcon.DictReader(f)

    # Insert data from CSV to database
    cur = conn.cursor()
    for row in csv_reader:
        age_desc = row['AGE_DESC']
        marital_status_code = row['MARITAL_STATUS_CODE']
        income_desc = row['INCOME_DESC']
        homeowner_desc = row['HOMEOWNER_DESC']
        hh_comp_desc = row['HH_COMP_DESC']
        household_size_desc = row['HOUSEHOLD_SIZE_DESC']
        kid_category_desc = row['KID_CATEGORY_DESC']
        household_key = row['household_key']

        cur.execute("""INSERT INTO hh_demographic 
            (age_desc, marital_status_code, income_desc, homeowner_desc, 
             hh_comp_desc, household_size_desc, kid_category_desc, household_key) 
             VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                    (age_desc, marital_status_code, income_desc, homeowner_desc,
                     hh_comp_desc, household_size_desc, kid_category_desc, household_key))

conn.commit()
cur.close()
conn.close()