-- What stocks does a specific ARK hold today? 
SELECT 
	dt,
	(SELECT stocks."name" FROM stocks WHERE stocks.id = etf_holding.etf_id) AS etf,
	(SELECT stocks.symbol FROM stocks WHERE stocks.id = etf_holding.etf_id) AS etf_symbol,
	shares,
	weight,
	stocks.symbol,
	stocks."name" 
FROM etf_holding


JOIN stocks 
ON etf_holding.holding_id = stocks.id

WHERE dt = CURRENT_DATE-1 
	AND (SELECT stocks.symbol FROM stocks WHERE stocks.id = etf_holding.etf_id) IN 
		('ARKG', 'ARKQ', 'ARKF', 'ARKK', 'ARKW', 'IZRL', 'PRNT')

-- What stocks did ARK buy more shares of? 
SELECT 
	today.dt,
	(SELECT stocks."name" FROM stocks WHERE stocks.id = today.etf_id) AS etf,
	today.shares,
	yesterday.shares,
	(today.shares - yesterday.shares) AS net_shares,
	today.weight,
	stocks.id,
	stocks.symbol,
	stocks."name" 
FROM etf_holding today, etf_holding yesterday 


JOIN stocks 
ON holding_id = stocks.id

WHERE today.dt = CURRENT_DATE-1 
	AND today.etf_id = yesterday.etf_id 
	AND today.holding_id = yesterday.holding_id 
	AND today.dt-1 = yesterday.dt
	AND (today.shares - yesterday.shares) > 0

-- What stocks did ARK decrease position in? 
SELECT 
	today.dt,
	(SELECT stocks."name" FROM stocks WHERE stocks.id = today.etf_id) AS etf,
	today.shares,
	yesterday.shares,
	(today.shares - yesterday.shares) AS net_shares,
	today.weight,
	stocks.id,
	stocks.symbol,
	stocks."name" 
FROM etf_holding today, etf_holding yesterday 


JOIN stocks 
ON holding_id = stocks.id

WHERE today.dt = CURRENT_DATE-1 
	AND today.etf_id = yesterday.etf_id 
	AND today.holding_id = yesterday.holding_id 
	AND today.dt-1 = yesterday.dt
	AND (today.shares - yesterday.shares) < 0

-- What stocks did ARK completely sell off?
SELECT 
	dt,
	(SELECT stocks."name" FROM stocks WHERE stocks.id = sold_stocks.etf_id) AS etf,
	shares,
	weight,
	stocks.id,
	stocks.symbol,
	stocks."name" 
FROM etf_holding sold_stocks


JOIN stocks 
ON holding_id = stocks.id

WHERE dt = CURRENT_DATE-2 
	AND NOT EXISTS (SELECT etf_id, holding_id FROM etf_holding todays_holdings
					WHERE sold_stocks.etf_id = todays_holdings.etf_id 
					AND sold_stocks.holding_id = todays_holdings.holding_id
					AND today.dt = CURRENT_DATE-1)

-- What new stocks did ARK add today? 
SELECT 
	dt,
	(SELECT stocks."name" FROM stocks WHERE stocks.id = bought_stocks.etf_id) AS etf,
	shares,
	weight,
	stocks.id,
	stocks.symbol,
	stocks."name" 
FROM etf_holding bought_stocks


JOIN stocks 
ON holding_id = stocks.id

WHERE dt = CURRENT_DATE-1 
	AND NOT EXISTS (SELECT etf_id, holding_id FROM etf_holding yesterdays_holdings
					WHERE bought_stocks.etf_id = yesterdays_holdings.etf_id 
					AND bought_stocks.holding_id = yesterdays_holdings.holding_id
					AND yesterdays_holdings.dt = CURRENT_DATE-2)

-- ARK ETF totals trends

-- Compare number of daily wsb mentions to daily stock price

SELECT 
	DATE(stock_mentions.dt), 
	stocks.symbol, COUNT(*) AS mentions, 
	MAX(stock_price.high) AS stock_price,
	MAX(volume) AS volume
FROM stock_mentions

JOIN stocks
ON stock_mentions.stock_id = stocks.id

JOIN stock_price
ON stock_mentions.stock_id = stock_price.stock_id
AND DATE(stock_price.dt) = DATE(stock_mentions.dt)

WHERE stocks.symbol IN ('stock symbol')

GROUP BY stocks.symbol, DATE(stock_mentions.dt)

ORDER BY DATE(stock_mentions.dt);