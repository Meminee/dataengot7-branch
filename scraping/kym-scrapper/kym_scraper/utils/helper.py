import psycopg2


class PostgresHelper:
    def __init__(self, dbname, user, password, host, port):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.conn = None
        self.cur = None

        # Connect to the database
        self.connect()

    def connect(self):
        self.conn = psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
        )
        self.cur = self.conn.cursor()

    def execute(self, query):
        self.cur.execute(query)
        self.conn.commit()
        return self.cur.fetchall() if self.cur.rowcount > 0 else None

    def execute_many(self, query, data):
        self.cur.executemany(query, data)
        self.conn.commit()

    def close_connection(self):
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
