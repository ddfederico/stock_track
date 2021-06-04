from psaw import PushshiftAPI
import datetime
import psycopg2
import psycopg2.extras
import re
import datetime as dt
import credential_collector

#CONNECT TO DATABASE
db_connection = psycopg2.connect(host=credential_collector.DB_HOST, 
                                database=credential_collector.DB_NAME, 
                                user=credential_collector.DB_USER, 
                                password=credential_collector.DB_PASS)

db_query = db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

#QUERY STOCKS TABLE IN DATABASE
db_query.execute("""
    SELECT * FROM stocks
    """)
db_rows = db_query.fetchall()

#ADD CASHTAG TO STOCKS IN DATABASE TO MATCH VALUES WITH REDDIT POSTS

stocks = {}
for row in db_rows:
    stocks['$' + row['symbol']] = row['id']

#GET MAX DATE FROM STOCK MENTIONS
db_query.execute("""
    SELECT MAX(dt) FROM stock_mentions
    WHERE source = 'reddit' AND sub_source = 'wallstreetbets'
    """)
#USE MAX DATE FROM STOCK MENTIONS FOR START TIME OF NEW WSB QUERY
start_date = db_query.fetchone()[0].date() + dt.timedelta(days=1)
start_time = int(dt.datetime(start_date.year, start_date.month, start_date.day).timestamp())

end_time = int(dt.datetime(dt.date.today().year, dt.date.today().month, dt.date.today().day).timestamp())

#PUSH SHIFT API USAGE TO SCRAPE REDDIT DATA
reddit_connector = PushshiftAPI()

wsb_posts = reddit_connector.search_submissions(
                            after=start_time,
                            before=end_time,
                            subreddit='wallstreetbets',
                            filter=['url','author', 'title', 'subreddit'],
                            )
#ONLY LOOK AT POSTS WITH A WORD THAT STARTS WITH '$' 
count_stocks = 0
for post in wsb_posts:
    words = post.title.split()
    cashtags = list(set(filter(lambda word: word.lower().startswith('$'), words)))

    if len(cashtags) > 0:
        print(cashtags)

#REMOVE PUNCTIATION FROM WORDS           
        for cashtag in cashtags:
            clean_cashtag = '$' + re.sub('\W+','', cashtag)
            if clean_cashtag in stocks:
                submitted_time = datetime.datetime.fromtimestamp(post.created_utc).isoformat()

#INSERT MATCHING WORD INTO DATABASE AS STOCK MENTION
                try: 
                    db_query.execute("""
                        INSERT INTO stock_mentions (stock_id, dt, message, source, sub_source, url)
                        VALUES (%s, %s, %s, 'reddit', 'wallstreetbets', %s)
                    """, (stocks[clean_cashtag], submitted_time, post.title, post.url))
                    print(f'INSERTING {clean_cashtag}:{stocks[clean_cashtag]} into stocks table')
                    count_stocks += 1 
                    db_connection.commit()
                except Exception as e:
                    print(e)
                    db_connection.rollback()

print(f'A total of {count_stocks} stocks were inserted into the database.')