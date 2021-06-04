import csv
import psycopg2
import psycopg2.extras
from datetime import date
import requests
import credential_collector

#CONNECT TO DATABASE
db_connection = psycopg2.connect(host=credential_collector.DB_HOST, 
                                database=credential_collector.DB_NAME, 
                                user=credential_collector.DB_USER, 
                                password=credential_collector.DB_PASS)

db_query = db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

db_query.execute("SELECT * FROM stocks WHERE is_etf = TRUE")

etfs = db_query.fetchall()

today = date.today()

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15'}

for etf in etfs:
    print(etf['symbol'])
    if etf['symbol'] == 'ARKK':
        csv_url = 'https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_INNOVATION_ETF_ARKK_HOLDINGS.csv'
    if etf['symbol'] == 'ARKG':
        csv_url = 'https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_GENOMIC_REVOLUTION_MULTISECTOR_ETF_ARKG_HOLDINGS.csv'
    if etf['symbol'] == 'ARKQ':
        csv_url = 'https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_AUTONOMOUS_TECHNOLOGY_&_ROBOTICS_ETF_ARKQ_HOLDINGS.csv'
    if etf['symbol'] == 'ARKF':
        csv_url = 'https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_FINTECH_INNOVATION_ETF_ARKF_HOLDINGS.csv'
    if etf['symbol'] == 'ARKW':
        csv_url = 'https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_NEXT_GENERATION_INTERNET_ETF_ARKW_HOLDINGS.csv'
    if etf['symbol'] == 'IZRL':
        csv_url = 'https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_ISRAEL_INNOVATIVE_TECHNOLOGY_ETF_IZRL_HOLDINGS.csv'
    if etf['symbol'] == 'PRNT':
        csv_url = 'https://ark-funds.com/wp-content/fundsiteliterature/csv/THE_3D_PRINTING_ETF_PRNT_HOLDINGS.csv'
    if etf['symbol'] == 'ARKX':
        csv_url ='https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_SPACE_EXPLORATION_&_INNOVATION_ETF_ARKX_HOLDINGS.csv'

    with requests.Session() as s:
        download = s.get(csv_url, headers=headers)

        decoded_content = download.content.decode('utf-8')

        reader = csv.reader(decoded_content.splitlines(), delimiter=',')
        next(reader)
        
        for row in reader:
            ticker = row[3]

            if ticker:
                shares = row[5]
                weight = row[7]
            
                db_query.execute("""
                    SELECT * FROM stocks WHERE symbol = %s
                """, (ticker,))
                stock = db_query.fetchone()
                if stock: 
                    db_query.execute("""
                        INSERT INTO etf_holding (etf_id, holding_id, dt, shares, weight)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (etf['id'], stock['id'], today, shares, weight))
db_connection.commit()