import pymysql
import traceback


class MysqlClient:
    def __init__(self, db_conf):
        self.db_conf = db_conf
        self.conn = self.make_db_connection()

    def make_db_connection(self):
        conn = pymysql.connect(**self.db_conf, charset='utf8mb4')
        return conn

    def insert(self, table_name, record):
        columns, values = list(record.keys()), list(record.values())
        sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s' for _ in values])});"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(sql, values)
            self.conn.commit()
        except Exception as e:
            traceback.print_exc()
            self.conn = self.make_db_connection()

    def execute(self, sql):
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(sql)
            self.conn.commit()
        except Exception as e:
            traceback.print_exc()
            self.conn = self.make_db_connection()


MariadbClient = MysqlClient
