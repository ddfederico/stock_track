import credentials
import yfinance
import psycopg2
import psycopg2.extras
import pandas
import datetime
from datetime import date

#CONNECT TO DATABASE
db_connection = psycopg2.connect(host=credentials.DB_HOST, database=credentials.DB_NAME, user=credentials.DB_USER, password=credentials.DB_PASS)

db_query = db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

#QUERY DATABASE
db_query.execute("""
                SELECT stocks.*, MAX(stock_price.dt) AS dt FROM stocks
                    
                LEFT JOIN stock_price
                ON stocks.id = stock_price.stock_id

                WHERE stock_price.dt IS NOT null OR date_added > CURRENT_DATE-7 

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
        historical_prices.insert(1, 'stock_id', stock['id'])
    
#INSERT DATA TO DATABASE    
        for index, row in historical_prices.iterrows():
            try:
                db_query.execute("""
                    INSERT INTO stock_price (stock_id, dt, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (row['stock_id'], index, row['Open'], row['High'], row['Low'], row['Close'], row['Volume'])
                    )
            except Exception as e:
                print(e)
                db_connection.rollback()
        print(f"Inserting {stock['symbol']}: {stock['id']} price data into database")
        db_connection.commit()
