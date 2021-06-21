# stockdb.py

import mariadb as db

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
    "INDEX `ticker` (`ticker` ASC), "
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
    "  `open_price` DECIMAL(11,6) NULL DEFAULT NULL,"
    "  `high_price` DECIMAL(11,6) NULL DEFAULT NULL,"
    "  `low_price` DECIMAL(11,6) NULL DEFAULT NULL,"
    "  `close_price` DECIMAL(11,6) NULL DEFAULT NULL,"
    "  `adj_close_price` DECIMAL(11,6) NULL DEFAULT NULL,"
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

        self.init_database()
        pass

    def __delattr__(self, name: str) -> None:
        self.conn.close()
        pass

    def init_database(self) -> None:
        cursor = self.conn.cursor()
        try:
            sql = "CREATE DATABASE IF NOT EXISTS {} DEFAULT CHARACTER SET 'utf8'".format(NAME_OF_DATABASE)
            cursor.execute(sql)
        except db.Error as err:
            print(err)
        
        try:
            cursor.execute("USE {}".format(NAME_OF_DATABASE))
        except db.Error as err:
            print(err)

        for table_name in TABLES:
            table_description = TABLES[table_name]
            try:
                print("Creating table {}: ".format(table_name), end='')
                cursor.execute(table_description)
            except db.Error as err:
                print(err)
            else:
                print("OK")
            
        cursor.close()
        pass

    def addingExchange(self, name, currency) -> None:
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
        else:
            print("Added exchange:{} currency:{}", name, currency)
        self.conn.commit()
        cursor.close()
        pass

# End of StockDB

def main():
    stockdb = StockDB()
    stockdb.addingExchange("KRX", "KRW")
# End of main

if __name__ == "__main__":
    main()
