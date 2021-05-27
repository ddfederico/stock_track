from time import time
from numpy.lib.function_base import insert
import credentials
import yfinance
import psycopg2
import psycopg2.extras
import pandas
import datetime
from datetime import date
import time

#CONNECT TO DATABASE
db_connection = psycopg2.connect(host=credentials.DB_HOST, database=credentials.DB_NAME, user=credentials.DB_USER, password=credentials.DB_PASS)

db_query = db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

#QUERY DATABASE
db_query.execute("""
                SELECT stocks.*, MAX(stock_price.dt) AS dt FROM stocks
                    
                LEFT JOIN stock_price
                ON stocks.id = stock_price.stock_id

                WHERE stock_price.dt IS NOT null OR date_added > CURRENT_DATE-21 

                GROUP BY id, symbol

                ORDER BY id
            """)

stocks = db_query.fetchall()

#ASSIGN END TIME FOR COLLECTING PRICE DATA
if date.today().weekday() == 5: 
    end_date = date.today() - datetime.timedelta(days=1)
elif date.today().weekday() == 6: 
    end_date = date.today() - datetime.timedelta(days=2)
else: 
    end_date = date.today()


for stock in stocks:
    start = time.time()
#ASSIGN START DATE FOR COLLECTING START DATE 
    if stock['dt']:
        start_date = stock['dt'].date() + datetime.timedelta(days=1)
    else: 
        start_date = datetime.date(2021, 1, 1)

    if start_date < end_date:
#GET PRICE DATA FROM YAHOO FINANCE
        historical_prices = yfinance.download(stock['symbol'],
                    interval='1d', 
                    start=start_date, 
                    end=end_date,
                    actions=True, 
                    progress=False
                    )
        

#INSERT STOCK ID FROM DATABASE                      
        if historical_prices.empty == False:
            historical_prices.insert(1, 'stock_id', stock['id'])
#DROP UNNECESSARY COLUMNS 
            drop_columns = historical_prices.drop(['Dividends', 'Stock Splits', 'Adj Close'], axis = 1)
#CONVERT DATAFRAME TO VALUES LIST         
            stock_price_data = drop_columns.reset_index().values.tolist()
        
            insert_query = 'INSERT INTO stock_price (dt, open, stock_id, high, low, close, volume) VALUES %s'

            try:
                psycopg2.extras.execute_values(db_query, insert_query, stock_price_data, template=None, page_size=100)
            except Exception as e:
                print(e)
                db_connection.rollback()
            print(f"Inserting {stock['symbol']}: {stock['id']} price data into database")
            print(round(time.time()-start, 2))
            db_connection.commit() 