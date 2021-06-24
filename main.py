import support as sp
import pandas as pd
import datetime
import sys
import pdb

total_trade = 0
kite = sp.kite
df20 = pd.read_csv('https://www1.nseindia.com/content/indices/ind_Nifty_Midcap_Liquid15.csv')
df20['nse'] = 'NSE:' + df20['Symbol']
watchlist = df20['Symbol'].tolist()
risk_per_trade = 100

temp = {'name':None, 'entry_price': None, 'buy_sell': None, 'qty': None, 'sl': None, 'traded':None, 'parent_order': None, 'sl_order':None, 'target':None, 'target_order': None}
status = {name: temp.copy() for name in watchlist}

while True:
	print("going for round 1")
	print(f"{datetime.datetime.now()}")
	try:
		for name in watchlist:
			df15, dfday = sp.get_live_date(name)
			ctime = datetime.datetime.now()
			completed_candle = pd.Series(datetime.datetime.now()).dt.floor('15min')[0] - datetime.timedelta(minutes=15)
			completed_candle = completed_candle.strftime("%Y-%m-%d %H:%M:%S+05:30")
			buy_condition, sell_condition = sp.check_entry(df15, name, dfday, completed_candle)
			row = df15.loc[completed_candle]
			# Buy Entry
			if (buy_condition) and (status[name]['traded'] is None) and (total_trade <= 4):
				print(f"Buy {name}")
				try:
					status[name]['name'] = name
					status[name]['entry_price'] = row['high']
					status[name]['buy_sell'] = 'buy'
					status[name]['target'] = round(round((row['high'] + ((row['high'] - row['low'])*2.1))/5,2)*5,2)
					status[name]['sl'] = row['low']
					status[name]['qty'] = 1
					#status[name]['qty'] = int(risk_per_trade//(row['high'] - row['low']))
					parent_order = kite.place_order(variety = kite.VARIETY_REGULAR, exchange = kite.EXCHANGE_NSE, tradingsymbol = name, transaction_type = kite.TRANSACTION_TYPE_BUY, quantity= status[name]['qty'], product = kite.PRODUCT_MIS, order_type = kite.ORDER_TYPE_SLM, price=None, validity=None, disclosed_quantity=None, trigger_price=status[name]['entry_price'], squareoff=None, stoploss=None, trailing_stoploss=None, tag=None)
					status[name]['parent_order'] = parent_order
					status[name]['traded'] = 'yes'
					total_trade = total_trade + 1
				except Exception as e:
					print(f"Error in buy {e}")
					continue
			# Sell Entry
			if (sell_condition) and (status[name]['traded'] is None) and (total_trade <= 4):
				print(f"Sell {name}")
				try:
					status[name]['name'] = name
					status[name]['entry_price'] = row['low']
					status[name]['buy_sell'] = 'sell'
					status[name]['target'] = round(round((row['low'] - ((row['high'] - row['low'])*2.1))/5,2)*5,2)
					status[name]['sl'] = row['high']
					status[name]['qty'] = 1
					#status[name]['qty'] = int(risk_per_trade//(row['high'] - row['low']))
					parent_order = kite.place_order(variety = kite.VARIETY_REGULAR, exchange = kite.EXCHANGE_NSE, tradingsymbol = name, transaction_type = kite.TRANSACTION_TYPE_SELL, quantity= status[name]['qty'], product = kite.PRODUCT_MIS, order_type = kite.ORDER_TYPE_SLM, price=None, validity=None, disclosed_quantity=None, trigger_price=status[name]['entry_price'], squareoff=None, stoploss=None, trailing_stoploss=None, tag=None)
					status[name]['parent_order'] = parent_order
					status[name]['traded'] = 'yes'
					total_trade = total_trade + 1
				except Exception as e:
					print(f"Error in sell {e}")
					continue    
			
			# Stop Loss order for parent order    
			if (status[name]['traded'] == 'yes') and (status[name]['sl_order'] is None):
				parent_order_details = kite.order_history(order_id = status[name]['parent_order'])[-1]
				if (parent_order_details['status'] == 'COMPLETE') and (status[name]['buy_sell'] == 'buy'):    
					try:
						sl_order = kite.place_order(variety = kite.VARIETY_REGULAR, exchange = kite.EXCHANGE_NSE, tradingsymbol = name, transaction_type = kite.TRANSACTION_TYPE_SELL, quantity= status[name]['qty'], product = kite.PRODUCT_MIS, order_type = kite.ORDER_TYPE_SLM, price=None, validity=None, disclosed_quantity=None, trigger_price=status[name]['sl'], squareoff=None, stoploss=None, trailing_stoploss=None, tag=None)
						status[name]['sl_order'] = 'sl_order'
					except Exception as e:
						print(f"Error in Buy SL {e}")
						continue
				
				if (parent_order_details['status'] == 'COMPLETE') and (status[name]['buy_sell'] == 'sell'):    
					try:
						sl_order = kite.place_order(variety = kite.VARIETY_REGULAR, exchange = kite.EXCHANGE_NSE, tradingsymbol = name, transaction_type = kite.TRANSACTION_TYPE_BUY, quantity= status[name]['qty'], product = kite.PRODUCT_MIS, order_type = kite.ORDER_TYPE_SLM, price=None, validity=None, disclosed_quantity=None, trigger_price=status[name]['sl'], squareoff=None, stoploss=None, trailing_stoploss=None, tag=None)
						status[name]['sl_order'] = 'sl_order'
					except Exception as e:
						print(f"Error in Sell SL {e}")
						continue

				# Target order for parent order    
			if status[name]['traded'] == 'yes' and (status[name]['target_order'] is None) :
				parent_order_details = kite.order_history(order_id = status[name]['parent_order'])[-1]
				if (parent_order_details['status'] == 'COMPLETE') and (status[name]['buy_sell'] == 'buy'):    
					try:
						target_order = kite.place_order(variety = kite.VARIETY_REGULAR, exchange = kite.EXCHANGE_NSE, tradingsymbol = name, transaction_type = kite.TRANSACTION_TYPE_SELL, quantity= status[name]['qty'], product = kite.PRODUCT_MIS, order_type = kite.ORDER_TYPE_LIMIT, price=status[name]['target'], validity=None, disclosed_quantity=None, trigger_price= None, squareoff=None, stoploss=None, trailing_stoploss=None, tag=None)
						status[name]['target_order'] = 'target_order'
					except Exception as e:
						print(f"Error in buy target {e}")
						continue
				
				if (parent_order_details['status'] == 'COMPLETE') and (status[name]['buy_sell'] == 'sell'):    
					try:
						target_order = kite.place_order(variety = kite.VARIETY_REGULAR, exchange = kite.EXCHANGE_NSE, tradingsymbol = name, transaction_type = kite.TRANSACTION_TYPE_BUY, quantity= status[name]['qty'], product = kite.PRODUCT_MIS, order_type = kite.ORDER_TYPE_LIMIT, price=status[name]['target'], validity=None, disclosed_quantity=None, trigger_price= None, squareoff=None, stoploss=None, trailing_stoploss=None, tag=None)
						status[name]['target_order'] = 'target_order'
					except Exception as e:
						print(f"Error in Sell target {e}")
						continue
			#Exit if 3:15			
			if ctime.time() > datetime.time(15, 15):
				sp.exit_funct()
				print("Exiting Now\nGood Day")
				sys.exit()			
	except Exception as e:
		print(f"Error in outer loop {e}")
		continue                    