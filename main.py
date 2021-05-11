# Be sure to pip install polygon-api-client

from datetime import datetime
from polygon import RESTClient
import sys, time, math
import db_conn


infunc = False

'''
    'ticket':{
       "ohlcv": [
            [
                1543572000000,
                4239.4,
                4239.6,
                4079.6,
                4079.63478779,
                2993.45281556
            ],
            [
                1543575600000,
                4082.2,
                4082.2,
                4020.2,
                4033.5,
                3216.95571165
            ],
            [
                1543579200000,
                4035.6,
                4072.78348726,
                3965,
                4055.6,
                2157.50135341
            ],
        ]
        }
    }
'''

#data = {'MSFT':{'ohlcv': []},'GOOGL':{'ohlcv': []},'EBAY':{'ohlcv': []},'COST':{'ohlcv': []},'WMT':{'ohlcv': []}}
#tickets = list(data.keys())
tickets = ['MSFT', 'COST', 'EBAY', 'WMT', 'GOOGL','AAPL']

def ts_to_datetime(ts) -> str:
    return datetime.fromtimestamp(ts / 1000.0).strftime('%Y-%m-%d %H:%M')


def datetime_to_ts(dt) -> str:
    return round(time.mktime(dt.timetuple()) ) * 1000  


def get_ticket(ticket, date_start, date_stop):
    return list(db_conn.select_ticket(ticket, date_start, date_stop))


def reload():
    global infunc
    res = ''
    key_phrase = "lGvIZVqDBu_ARh9vIXo_XF61KDJ82lWF"

    # RESTClient can be used as a context manager to facilitate closing the underlying http session
    # https://requests.readthedocs.io/en/master/user/advanced/#session-objects
    #MSFT, COST, EBAY, WMT, GOOGL
    if infunc == True:
        return 'Wait previus update....'
    
    infunc = True
    try:
        rest = 0
        for ticket in tickets:
        
            ohlcv = []  
            start_time, last_time = db_conn.select_last_time(ticket)
            
            #get_ticket(ticket)#  data[ticket]['ohlcv']
            
            with RESTClient(key_phrase) as client:
                endday = False
                pred_from = 0
                while endday == False:
                    if last_time:
                        from_ = last_time + 60000 # ohlcv[-1][0] + 60000
                    else:
                        from_ = "2020-04-01"
                        from_ = datetime_to_ts(datetime.strptime(from_, "%Y-%m-%d"))

                    
                    to = datetime_to_ts(datetime.today())
                    if from_ >= to or from_ <= pred_from:
                        endday = True
                        break
                    pred_from = from_
                    try:
                        resp = client.stocks_equities_aggregates(ticket, 1, "minute", from_, to, unadjusted=False, limit=49000)
                    except (IOError, Exception) as e:
                        
                        print(e)
                        #temp = sys.exc_info()[0]
                        res = "error: " + str(e) # temp.__doc__
                        print(res)
                        if '429 Client Error: Too Many Requests for' in res:
                            time.sleep(60)
                            pred_from = 0 
                            continue

                    print(f"Minute aggregates for {resp.ticker} between {from_} and {to}.")
                    
                    

                    if resp.resultsCount > 0:
                        for result in resp.results:
                            #dt = ts_to_datetime(result["t"])
                            #print(f"{dt}\n\tO: {result['o']}\n\tH: {result['h']}\n\tL: {result['l']}\n\tC: {result['c']} ")
                            bar = []
                            bar.append(result["t"])
                            bar.append(result['o'])
                            bar.append(result['h'])
                            bar.append(result['l'])
                            bar.append(result['c'])
                            bar.append(result['v'])

                            ohlcv.append(bar)
                        last_time = ohlcv[-1][0]
            if len(ohlcv) > 0:
                db_conn.insert_data(ticket, ohlcv)
            #data[ticket]['ohlcv'] = ohlcv
            rest += len(ohlcv)
        res = rest
    except (IOError, Exception) as e:
        print(e)
        #temp = sys.exc_info()[0]
        res = "error: " + str(e) # temp.__doc__
        print(res)
    infunc = False
    return res


def create_tables():
    for ticket in tickets:
        db_conn.create_table(ticket)


if __name__ == "__main__":
    reload()
    #create_tables()
    