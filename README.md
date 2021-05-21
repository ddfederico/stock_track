# Stock Track Project

## Overview
The purpose of this project is to automate the collection and storage of key data relating to stocks. Once the data is being collected and stored in 
our database, we will analyze the data in order to develop insights that may influence personal investment strategies. The project started with a curiosity of a the possible correlation between the number of times a stock was mention in r/wallstreetbets and the stock's price. I was curious to see if I could predict another GME short squeeze situation by stock mentions in subreddit. 
</br>There are 6 main pieces to this project:</br>
1. Collection of stock names and tickers <i>(populate_stocks.py)<i>
2. Collection of stock prices <i>(populate_stock_prices.py)<i>
3. Collection ARK ETF fund breakdown of holdings <i>(populate_ark_data.py)<i>
4. Collection of Reddit r/wallstreetbets stock mentions <i>(wsb_collector.py)<i>
5. SQL queries to answer question about data <i>(sql_queries.sql)<i>
6. Create digestible data visualizations using matplotlib <i>(in progress...)<i>

## Prerequisites
1. Set up a database <i>(I used a docker container with postgres sql install on top of it)<i>
2. Create an Alpaca account <i>(This is what I used in order to pull stock tickers)<i>
3. A SQL client <i>(I used TablePlus but there are many other option to choose from)<i> 
4. Import necessary python libraries
    * alpaca_trade_api
    * psycopg2
    * yfinance 
    * psaw
    * pandas 

## What I learned
  * How to set up and use Docker
  * How to install Postgres SQL onto a Docker container in order to create a database 
  * The following python librries: yfinance, psaw, psycopg2, pandas
  * GitHub 

## Sources 
Some of the code and ideas in this project were learned and used from Youtube videos. 
### Youtube Links:
   * [Part Time Larry](https://www.youtube.com/c/parttimelarry/featured)
   * [Tracking ARK Invest ETFs with Python and PostgreSQL](https://www.youtube.com/watch?v=5uW0TLHQg9w)
   * [Tracking WallStreetBets Stocks with Python, Reddit API, and SQL](https://www.youtube.com/watch?v=CJAdCLZaISw)
### GitHub Links:
   * [ark-funds-tracker](https://github.com/hackingthemarkets/ark-funds-tracker)
   * [wallstreetbets-tracker](https://github.com/hackingthemarkets/wallstreetbets-tracker/blob/main/search_wsb.py)
