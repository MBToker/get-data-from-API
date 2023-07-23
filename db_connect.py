import psycopg2
import psycopg2.extras
import pandas as pd

#Getting configuration data from .txt file
configuration_values = {}
with open('configure.txt') as f:
    for line in f:
        line = line.strip()
        splitted_line = line.split(" ")
        configuration_values[splitted_line[0]] = splitted_line[2]
configuration_values['port_id'] = int(configuration_values['port_id'])

connect_to_db = None
cursor = None

#Function that used for table creations and queries
def create_table(query:str):
    try:
        with psycopg2.connect(
            host=configuration_values['hostname'],
            dbname=configuration_values['database'],
            user=configuration_values['username'],
            password=configuration_values['pwd'],
            port=configuration_values['port_id']
        )as connect_to_db:
            with connect_to_db.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.execute(query)

    except Exception as error:
        print(error)

# Function that used for insertions. 
# If last parameter is true, it is insertion for last_transactions table. Otherwise, insertion for other table
def insert_to_table(df, insert_script:str, isLast_transaction:bool):
    try:
        with psycopg2.connect(
            host=configuration_values['hostname'],
            dbname=configuration_values['database'],
            user=configuration_values['username'],
            password=configuration_values['pwd'],
            port=configuration_values['port_id']
        )as connect_to_db:
            with connect_to_db.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                
                for index in range(len(df)):
                    if isLast_transaction:
                        insert_value = (df.at[index,'currency'],df.at[index,'amount'], df.at[index,'price'])
                    else:
                        insert_value = (df.at[index,'currency'], df.at[index,'buyers_or_sellers'],
                                    df.at[index,'orders_total_amount'], df.at[index,'orders_price'])
                    cursor.execute(insert_script, insert_value)

    except Exception as error:
        print(error)

# Function for select operations from table
def select_from_table(query:str):
    try:
        with psycopg2.connect(
            host=configuration_values['hostname'],
            dbname=configuration_values['database'],
            user=configuration_values['username'],
            password=configuration_values['pwd'],
            port=configuration_values['port_id']
        )as connect_to_db:
            with connect_to_db.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                df = pd.read_sql_query(query,con=connect_to_db)
                return df


    except Exception as error:
        print(error)
