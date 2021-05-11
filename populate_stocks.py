import credentials
import alpaca_trade_api as tradeapi
import psycopg2
import psycopg2.extras
from datetime import date

db_connection = psycopg2.connect(host=credentials.DB_HOST, database=credentials.DB_NAME, user=credentials.DB_USER, password=credentials.DB_PASS)

db_query = db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

api_connection = tradeapi.REST(credentials.API_KEY, credentials.API_SECRET, base_url=credentials.API_URL)

stocks = api_connection.list_assets()

today = date.today()

db_query.execute("SELECT symbol FROM stocks")

existing_nested_stocks = db_query.fetchall()
existing_stocks = [stock for sublist in existing_nested_stocks for stock in sublist]

for stock in stocks:
    if stock.symbol not in existing_stocks:
            print(f"Inserting stock {stock.name} {stock.symbol}")
            db_query.execute("""
               INSERT INTO stocks (name, symbol, exchange, is_etf, date_added)
               VALUES (%s, %s, %s, false, %s)
                """, (stock.name, stock.symbol, stock.exchange, today))

db_connection.commit()