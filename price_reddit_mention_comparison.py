import psycopg2
import psycopg2.extras
import pandas
import matplotlib.pyplot as plt
import pandas
import numpy as np
import matplotlib.dates as mdates
from matplotlib.dates import DateFormatter
import credential_collector

#CONNECT TO DATABASE
db_connection = psycopg2.connect(host=credential_collector.DB_HOST, 
                                database=credential_collector.DB_NAME, 
                                user=credential_collector.DB_USER, 
                                password=credential_collector.DB_PASS)

db_query = db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

#QUERY DATABASE
db_query.execute("""
SELECT 
	DATE(stock_mentions.dt), 
	stocks.symbol, COUNT(*) AS mentions, 
	MAX(stock_price.high) AS stock_price,
	MAX(volume) AS volume
FROM stock_mentions

JOIN stocks
ON stock_mentions.stock_id = stocks.id

RIGHT JOIN stock_price
ON stock_mentions.stock_id = stock_price.stock_id
AND DATE(stock_price.dt) = DATE(stock_mentions.dt)

WHERE stocks.symbol IN ('GME')

GROUP BY stocks.symbol, DATE(stock_mentions.dt)

ORDER BY DATE(stock_mentions.dt)
            """)

data = db_query.fetchall()
df = pandas.DataFrame(data)
df = df.rename(columns = {0:'Date',1:'Ticker',2:'Mentions',3:'Price',4:'Volume'})
df = df.astype({'Date':'datetime64','Price':'float32','Volume':'int64'})
print(df)

fig, ax1 = plt.subplots()


ax1.set_xlabel('Date')
ax1.set_ylabel('Price')
ax1.plot(df['Date'], df['Price'], color='grey')

ax2 = ax1.twinx()
ax2.set_ylabel('Mentions')
ax2.plot(df['Date'], df['Mentions'], color='blue')

date_form = DateFormatter("%m-%d")
ax1.xaxis.set_major_formatter(date_form)

ax1.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))

plt.show()