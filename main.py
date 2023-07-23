import requests
import json
import pandas as pd
from db_connect import create_table, insert_to_table, select_from_table

# Functions
# Function that gets the data from API and puts data into a json format
def get_data_from_API(link:str):
    response_API = requests.get(link)
    data = response_API.text
    json_data = json.loads(data)
    return json_data

# Function that gets the specific part of the data
def get_specific_array(data:object, wanted_part:str, currency:str, list_to_append:list):
    result = data['data'][wanted_part]
    result=pd.DataFrame(result)

    if wanted_part=='buyers':
        result['buyers_or_sellers']='buyers'

    elif wanted_part=='sellers':
        result['buyers_or_sellers']='sellers'

    result['currency']=currency
    list_to_append.append(result)

#Function that used for the create script of select queries
def script_for_select(currency:str, table_name:str, col1:str, col2:str):
    result="""SELECT 
            min(%s) AS min_amount,
            min(%s) AS min_price,
            max(%s) AS max_amount,
            max(%s) AS max_price,
            avg(%s) AS avg_amount,
            avg(%s) AS avg_price,
            sum(%s)/24 AS volume_amount,
            sum(%s)/24 AS volume_price
            FROM %s
            WHERE currency='%s';"""  %(col1, col2, col1, col2, col1, col2, col1, col2, table_name, currency)
    return result

def main():
    # Data process for buyers/sellers
    link_for_TRY = 'https://www.bitexen.com/api/v1/order_book/BTCTRY/'
    link_for_USDT = 'https://www.bitexen.com/api/v1/order_book/BTCUSDT/'

    TRY_data = get_data_from_API(link_for_TRY)
    USDT_data = get_data_from_API(link_for_USDT)

    buyers_and_sellers_list=[] # List that keeps all of the buyers and sellers data
    get_specific_array(TRY_data, 'buyers','TRY', buyers_and_sellers_list)
    get_specific_array(USDT_data, 'buyers', 'USDT', buyers_and_sellers_list)
    get_specific_array(TRY_data, 'sellers', 'TRY', buyers_and_sellers_list)
    get_specific_array(USDT_data, 'sellers', 'USDT', buyers_and_sellers_list)

    buyers_and_sellers_df=pd.concat(buyers_and_sellers_list,ignore_index=True) # Dataframe that combines all of the buyers and seller info


    last_transaction_list=[] # List that keeps all of the last transactions data
    TRY_last_transactions = get_specific_array(TRY_data, 'last_transactions', 'TRY', last_transaction_list)
    USDT_last_transactions = get_specific_array(USDT_data, 'last_transactions', 'USDT', last_transaction_list)

    last_transaction_df=pd.concat(last_transaction_list, ignore_index=True) # Dataframe that combines all of the last transactions info


    # Database processes for buyers and sellers
    # Create tables
    create_table_script = '''CREATE TABLE IF NOT EXISTS buyers_and_sellers(
        id SERIAL PRIMARY KEY,
        currency VARCHAR(10),
        buyers_or_sellers VARCHAR(10) NOT NULL,
        orders_total_amount DOUBLE PRECISION NOT NULL,
        orders_price DOUBLE PRECISION NOT NULL);'''
    create_table(create_table_script)

    create_table_script = '''CREATE TABLE IF NOT EXISTS last_transactions(
        id SERIAL PRIMARY KEY,
        currency VARCHAR(10),
        amount DOUBLE PRECISION NOT NULL,
        price DOUBLE PRECISION NOT NULL);'''
    create_table(create_table_script)


    # Insert dataframes into table
    insert_script = '''INSERT INTO buyers_and_sellers
                    (currency, buyers_or_sellers, orders_total_amount, orders_price)
                    VALUES(%s, %s, %s, %s);'''
    insert_to_table(buyers_and_sellers_df, insert_script, False)

    insert_script = '''INSERT INTO last_transactions
                    (currency, amount, price)
                    VALUES(%s, %s, %s);'''
    insert_to_table(last_transaction_df, insert_script, True)


    # Selecting queries and combining data
    # Data with TRY currency
    select_script=script_for_select("TRY","buyers_and_sellers", "orders_total_amount", "orders_price")
    buyers_sellers_query_result_TRY_df=select_from_table(select_script)
    buyers_sellers_query_result_TRY_df["data_from"]="buyers_and_sellers"

    select_script=script_for_select("TRY","last_transactions", "amount", "price")
    last_transactions_query_result_TRY_df=select_from_table(select_script)
    last_transactions_query_result_TRY_df["data_from"]="last_transactions"

    #Combine all of the TRY currency dataframes in one dataframe
    TRY_combined_data_df=pd.concat([buyers_sellers_query_result_TRY_df, last_transactions_query_result_TRY_df], ignore_index=True)


    # Data with USDT currency 
    select_script=script_for_select("USDT","buyers_and_sellers", "orders_total_amount", "orders_price")
    buyers_sellers_query_result_USDT_df=select_from_table(select_script)
    buyers_sellers_query_result_USDT_df["data_from"]="buyers_and_sellers"

    select_script=script_for_select("USDT","last_transactions", "amount", "price")
    last_transactions_query_result_USDT_df=select_from_table(select_script)
    last_transactions_query_result_USDT_df["data_from"]="last_transactions"

    #Combine all of the USDT currency dataframes in one dataframe
    USDT_combined_data_df=pd.concat([buyers_sellers_query_result_USDT_df, last_transactions_query_result_USDT_df], ignore_index=True)


    #Export results to excel files
    TRY_excel_path=r'C:\Users\Prime\OneDrive\Masaüstü\Klasör\Ders\Staj\Staj işlemleri\Staj-1\Dersler\Assignments\Assignment-2(Python)\TRY.xlsx'
    USDT_excel_path=r'C:\Users\Prime\OneDrive\Masaüstü\Klasör\Ders\Staj\Staj işlemleri\Staj-1\Dersler\Assignments\Assignment-2(Python)\USDT.xlsx'

    TRY_combined_data_df.to_excel(TRY_excel_path, sheet_name='TRY_sheet', index=False)
    USDT_combined_data_df.to_excel(USDT_excel_path, sheet_name='USDT_sheet', index=False)

if __name__=='__main__':
    main()