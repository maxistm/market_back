import psycopg2, time
from psycopg2 import Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from datetime import datetime
import sett


# Подключение к существующей базе данных
def connect():

    try:
        connection = psycopg2.connect(user=sett.db_param['user'],
                                    password=sett.db_param['password'],
                                    host=sett.db_param['host'],
                                    port=sett.db_param['port'],
                                    database=sett.db_param['database'])

        cursor = connection.cursor()
        return connection, cursor

    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)


def disconect(connection, cursor):
    if connection:
        cursor.close()
        connection.close()
        


def create_table(ticket):
    try:
        # Подключиться к существующей базе данных
        connection, cursor = connect()
        # SQL-запрос для создания новой таблицы
        create_table_query = '''CREATE TABLE IF NOT EXISTS '''+ticket+'''
                            (ts INT8 PRIMARY KEY  UNIQUE   NOT NULL,
                            open   REAL    NOT NULL,
                            high   REAL    NOT NULL,
                            low   	REAL    NOT NULL,
                            close   REAL    NOT NULL,
                            volume   INT    NOT NULL
                            );  '''
        # Выполнение команды: это создает новую таблицу
        cursor.execute(create_table_query)
        connection.commit()
        create_index_query = '''CREATE INDEX IF NOT EXISTS ts_idx ON '''+ticket+''' (ts);  '''
        # Выполнение команды: это создает новую таблицу
        cursor.execute(create_table_query)
        connection.commit()
        print("Таблица успешно создана  " + ticket)
        disconect(connection, cursor)
    except (Exception, Error) as error:
        print("Ошибка при работе ", error)

def ts_to_datetime(ts) -> str:
    return datetime.fromtimestamp(ts / 1000.0).strftime('%Y-%m-%d %H:%M')


def jsondatetime_to_ts(dt) -> str:
    dt = datetime.strptime(dt[:10], '%Y-%m-%d')
    return round(time.mktime(dt.timetuple()) ) * 1000  


def select_ticket(ticket, date_start, date_stop):
    try:
        connection, cursor = connect()
        date_start = jsondatetime_to_ts(date_start)
        date_stop = jsondatetime_to_ts(date_stop)
        create_select_query = 'SELECT * FROM '+ticket+' WHERE ts >= %s AND ts < %s ;'
        cursor.execute(create_select_query, (str(date_start), str(date_stop)))
        result = cursor.fetchall()
        disconect(connection, cursor)
        return result
    except (Exception, Error) as error:
        print("Ошибка при SELECT ", error)
        return []

def select_last_time(ticket):
    try:
        connection, cursor = connect()
        create_select_query = '''SELECT ts FROM '''+ticket+''' ORDER BY ts DESC LIMIT 1;'''
        cursor.execute(create_select_query)
        last_time = cursor.fetchall()
        create_select_query = '''SELECT ts FROM '''+ticket+''' ORDER BY ts ASC LIMIT 1;'''
        cursor.execute(create_select_query)
        start_time = cursor.fetchall()

        disconect(connection, cursor)
        if len(last_time) > 0:
            last_time = last_time[0][0]
        else:
            last_time = None

        if len(start_time) > 0:
            start_time = start_time[0][0]
        else:
            start_time = None
        return start_time, last_time
    except (Exception, Error) as error:
        print("Ошибка при SELECT ", error)
        return []

def insert_data(ticket, data):
    try:
        connection, cursor = connect()
        cursor.executemany('INSERT INTO '+ticket+'("ts", "open", "high", "low", "close", "volume") VALUES (%s, %s, %s, %s, %s, %s)', data )
        connection.commit()
        disconect(connection, cursor)

    except (Exception, Error) as error:
        print("Ошибка при SELECT ", error)

def clear_all(tickets):
    try:
        connection, cursor = connect()
        sql = ''
        for ticket in tickets:
            sql += ticket + ','
        sql = sql[:-1] + ';' 
        cursor.execute(' TRUNCATE ' + sql )
        connection.commit()
        disconect(connection, cursor)

    except (Exception, Error) as error:
        print("Ошибка при SELECT ", error)

        
if __name__ == "__main__":
    tickets = ['MSFT', 'COST', 'EBAY', 'WMT', 'GOOGL','AAPL']
    #clear_all(tickets)
    for ticket in tickets:
        create_table(ticket)
    