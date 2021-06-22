# stockdb.py
import datetime
import sys
import time
import mariadb as db
import numpy
import pandas as pd
import pandas_datareader as pdr

YAHOO_VENDOR_ID = 1
NAME_OF_DATABASE = 'stock_prices'
TABLES = {}
TABLES['exchange'] = (
    "CREATE TABLE IF NOT EXISTS `exchange` ("
    "`id` INT(11) NOT NULL AUTO_INCREMENT, "
    "`name` VARCHAR(10) NOT NULL, "
    "`currency` CHAR(3) NULL DEFAULT NULL, "
    "`created_date` DATETIME NULL DEFAULT CURRENT_TIMESTAMP(), "
    "`last_updated` DATETIME NULL DEFAULT CURRENT_TIMESTAMP() ON UPDATE CURRENT_TIMESTAMP(), "
    "PRIMARY KEY (`id`)) "
    "ENGINE = InnoDB"
)
TABLES['security'] = (
    "CREATE TABLE IF NOT EXISTS `security` ( "
    "`id` INT(11) NOT NULL AUTO_INCREMENT, "
    "`exchange_id` INT(11) NOT NULL, "
    "`ticker` VARCHAR(10) NOT NULL, "
    "`name` VARCHAR(100) NULL, "
    "`sector` VARCHAR(100) NULL, "
    "`industry` VARCHAR(100) NULL, "
    "`created_date` DATETIME NULL DEFAULT CURRENT_TIMESTAMP(), "
    "`last_updated` DATETIME NULL DEFAULT CURRENT_TIMESTAMP() ON UPDATE CURRENT_TIMESTAMP(), "
    "PRIMARY KEY (`id`), "
    "INDEX `exchange_id` (`exchange_id` ASC), "
    "UNIQUE INDEX `ticker` (`ticker` ASC), "
    "CONSTRAINT `fk_exchange_id`"
    "  FOREIGN KEY (`exchange_id`)"
    "  REFERENCES `exchange` (`id`)"
    "  ON DELETE NO ACTION"
    "  ON UPDATE NO ACTION) "
    "ENGINE = InnoDB"
)
TABLES['data_vendor'] = (
    "CREATE TABLE IF NOT EXISTS `data_vendor` ( "
    "`id` INT(11) NOT NULL AUTO_INCREMENT, "
    "`name` VARCHAR(32) NOT NULL, "
    "`website_url` VARCHAR(255) NULL DEFAULT NULL, "
    "`created_date` DATETIME NULL DEFAULT CURRENT_TIMESTAMP(), "
    "`last_updated` DATETIME NULL DEFAULT CURRENT_TIMESTAMP() ON UPDATE CURRENT_TIMESTAMP(), "
    "PRIMARY KEY (`id`)) "
    "ENGINE = InnoDB "
    "AUTO_INCREMENT = 3"
)
TABLES['daily_price'] = (
    "CREATE TABLE IF NOT EXISTS `daily_price` ("
    "  `id` INT(11) NOT NULL AUTO_INCREMENT,"
    "  `data_vendor_id` INT(11) NOT NULL,"
    "  `ticker_id` INT(11) NOT NULL,"
    "  `price_date` DATE NOT NULL,"
    "  `created_date` DATETIME NULL DEFAULT CURRENT_TIMESTAMP(),"
    "  `last_updated` DATETIME NULL DEFAULT CURRENT_TIMESTAMP() ON UPDATE CURRENT_TIMESTAMP(),"
    "  `open_price` DECIMAL(15,6) NULL DEFAULT NULL,"
    "  `high_price` DECIMAL(15,6) NULL DEFAULT NULL,"
    "  `low_price` DECIMAL(15,6) NULL DEFAULT NULL,"
    "  `close_price` DECIMAL(15,6) NULL DEFAULT NULL,"
    "  `adj_close_price` DECIMAL(15,6) NULL DEFAULT NULL,"
    "  `volume` BIGINT(20) NULL DEFAULT NULL,"
    "  PRIMARY KEY (`id`),"
    "  INDEX `price_date` (`price_date` ASC),"
    "  INDEX `ticker_id` (`ticker_id` ASC),"
    "  CONSTRAINT `fk_ticker_id`"
    "    FOREIGN KEY (`ticker_id`)"
    "    REFERENCES `security` (`id`)"
    "    ON DELETE NO ACTION"
    "    ON UPDATE NO ACTION,"
    "  CONSTRAINT `fk_data_vendor_id`"
    "    FOREIGN KEY (`data_vendor_id`)"
    "    REFERENCES `data_vendor` (`id`)"
    "    ON DELETE NO ACTION"
    "    ON UPDATE NO ACTION)"
    " ENGINE = InnoDB"
)

class StockDB():
    def __init__(self) -> None:
        try:
            self.conn = db.connect(
                user='trader',
                password='glasowk2',
                host='localhost',
                port=3306)
        except db.Error as err:
            print(err)
            sys.exit(1)

        self.init_database()
    # End of __init__

    def __delattr__(self, name: str) -> None:
        self.conn.close()
    # End of __delattr__

    def init_database(self) -> None:
        cursor = self.conn.cursor()
        try:
            sql = "CREATE DATABASE IF NOT EXISTS {} DEFAULT CHARACTER SET 'utf8'".format(NAME_OF_DATABASE)
            cursor.execute(sql)
        except db.Error as err:
            print(err)
            sys.exit(1)
        
        try:
            cursor.execute("USE {}".format(NAME_OF_DATABASE))
        except db.Error as err:
            print(err)
            sys.exit(1)

        for table_name in TABLES:
            table_description = TABLES[table_name]
            try:
                print("Creating table {}: ".format(table_name), end='')
                cursor.execute(table_description)
            except db.Error as err:
                print(err)
                sys.exit(1)
            else:
                print("OK")

        cursor.close()
    # End of init_database

    def read_security(self, exchange):
        df = None
        if exchange == 'KRX':
            df = pd.read_csv('data/KRX_5918_20210622.csv')
            df.drop(['표준코드', '한글 종목약명', '영문 종목명', '상장일',
                    '증권구분', '주식종류'], axis=1, inplace=True)
            df.columns = ['ticker', 'name', 'sector', 'industry', 'price', 'shares']
        return df
    # End of read_security

    def exchange(self, name, currency):
        cursor = self.conn.cursor()
        try:
            sql = (
                "INSERT INTO exchange (name, currency)"
                " SELECT * FROM (SELECT '{}' AS name, '{}' AS currency) AS temp"
                " WHERE NOT EXISTS (SELECT name FROM exchange WHERE name = '{}')"
                " LIMIT 1"
            ).format(name, currency, name)
            cursor.execute(sql)
        except db.Error as err:
            print(err)
            sys.exit(1)
        else:
            print("Added exchange:{} currency:{}".format(name, currency))
        self.conn.commit()
        
        try:
            sql = "SELECT id, name, currency FROM exchange WHERE name = '{}'".format(name)
            cursor.execute(sql)
        except db.Error as err:
            print(err)
            sys.exit(1)
        row = cursor.fetchone()
        if row is not None:
            exchange_id = row[0]
        else:
            print("Error!")

        df = self.read_security(name)
        if not df.empty:
            print("Number of {} tickers:{}".format(name, len(df)))
            df['exchange_id'] = exchange_id
            cols = df.columns.tolist()
            df = df[cols[-1:] + cols[:-1]]

            # do update security table
            for row in df.itertuples(index=False):
                try:
                    sql = (
                        "INSERT INTO security (exchange_id, ticker, name, sector, industry) "
                        "VALUES(%s, %s, %s, %s, %s) "
                        "ON DUPLICATE KEY UPDATE name = '{}', sector = '{}', industry = '{}'"
                    ).format(row[2], row[3], row[4])
                    cursor.execute(sql, row[:5])
                except:
                    # Assume that the exception is because sector
                    # and/or industry are missing
                    sql = (
                        "INSERT INTO security (exchange_id, ticker, name, sector) "
                        "VALUES(%s, %s, %s, %s) "
                        "ON DUPLICATE KEY UPDATE name = '{}', sector = '{}'"
                    ).format(row[2], row[3])
                    cursor.execute(sql, row[:4])
                self.conn.commit()
        else:
            print("Not found tickers on {}!".format(name))

        cursor.close()
    # End of exchange

    def reader(self, vendor, website):
        cursor = self.conn.cursor()
        try:
            sql = (
                "INSERT INTO data_vendor (name, website_url)"
                " SELECT * FROM (SELECT '{}' AS name, '{}' AS website_url) AS temp"
                " WHERE NOT EXISTS (SELECT name FROM data_vendor WHERE name = '{}')"
                " LIMIT 1"
            ).format(vendor, website, vendor)
            cursor.execute(sql)
        except db.Error as err:
            print(err)
            sys.exit(1)
        else:
            print("Added data vendor:{} website_url:{}".format(vendor, website))
        self.conn.commit()

        try:
            sql = "SELECT id, name, website_url FROM data_vendor WHERE name = '{}'".format(vendor)
            cursor.execute(sql)
        except db.Error as err:
            print(err)
            sys.exit(1)
        row = cursor.fetchone()
        if row is not None:
            vendor_id = row[0]
        else:
            print("Error!")
        cursor.close()

        self.download(vendor_id, "KOSPI")
    # End of reader

    def download(self, vendor_id, sector = None):
        if sector is not None:
            sql = "SELECT ticker, id FROM security WHERE sector = '{}'".format(sector)
        else:
            sql = "SELECT ticker, id FROM security"
        all_tickers = pd.read_sql(sql, self.conn)
        ticker_index = dict(all_tickers.to_dict('split')['data'])
        tickers = list(ticker_index.keys())

        # Get last date
        sql = "SELECT price_date FROM daily_price WHERE ticker_id={}".format(ticker_index[tickers[0]])
        dates = pd.read_sql(sql, self.conn)
        if not dates.empty:
            last_date = dates.iloc[-1, 0]
            if last_date == datetime.date.today():
                print("The data of daily prices is up to date.")
                return
            last_date = last_date + datetime.timedelta(days=1)
        else:
            today = datetime.date.today()
            last_date = datetime.date(today.year - 2, today.month, today.day)

        print(f"Updating daily prices data from {last_date}.")
        started = time.time()
        self.download_all_data(vendor_id, tickers, ticker_index, start_date=last_date)
        print(f"Updated the data. It took {time.time()-started} seconds.")
    # End of download

    def download_all_data(self, vendor_id, tickerlist, ticker_index,
                        chunk_size=100, start_date=None):
        # Hacky snippet to get the ceiling
        n_chunks = -(-len(tickerlist) // chunk_size)
        ms_tickers = []
        for i in range(0, n_chunks * chunk_size, chunk_size):
            # This will download data from the earliest possible date
            ms_from_chunk = self.download_data_chunk(i, i+chunk_size, 
                                                    vendor_id,
                                                    tickerlist,
                                                    ticker_index,
                                                    start_date)
            ms_tickers.append(ms_from_chunk)
            
            # Check for possible throttling
            if len(ms_from_chunk) > 40:
                time.sleep(120)
            else:
                time.sleep(10)
        return ms_tickers
    # End of download_all_data

    def download_data_chunk(self, start_idx, end_idx, vendor_id, tickerlist, ticker_index,
                            start_date=None):
        cursor = self.conn.cursor()
        ms_tickers = []
        for ticker in tickerlist[start_idx:end_idx]:
            ticker_id=[]
            ticker_id.append(ticker)
            ticker_id.append('.KS')
            ticker_id = "".join(ticker_id)
            df = pdr.get_data_yahoo(ticker_id, start=start_date)
            if df.empty:
                print(f"df is empty for {ticker}")
                ms_tickers.append(ticker)
                time.sleep(3)
                continue
            
            df.index = df.index.date
            for row in df.itertuples():
                values = [vendor_id, ticker_index[ticker]] + \
                        list(row)
                try:
                    sql = (
                        "INSERT INTO daily_price (data_vendor_id, "
                        "ticker_id, price_date, open_price, "
                        "high_price, low_price, close_price, "
                        "volume, adj_close_price) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    )
                    cursor.execute(sql, tuple(values))
                except Exception as e:
                    print(values)
                    print(str(e))
            self.conn.commit()
        cursor.close()
        return ms_tickers
    # End of download_data_chunk

# End of StockDB

def main():
    stockdb = StockDB()
    stockdb.exchange("KRX", "KRW")
    stockdb.reader("YahooFinance", "https://finance.yahoo.com")
# End of main

if __name__ == "__main__":
    main()
