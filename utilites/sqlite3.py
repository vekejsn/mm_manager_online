import sqlite3

class db_connection:
    def __init__(self, db_name):
        self.db_name = db_name
        self.conn = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def connect(self):
        self.conn = sqlite3.connect(self.db_name, check_same_thread=False)

    def execute(self, query, params=None):
        if params is None:
            params = ()
        while True:
            try:
                resp = self.conn.execute(query, params)
                break
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e):
                    self.connect()
                    continue
                else:
                    print('Query: ', query, 'Params: ', params)
                    raise e
            except Exception as e:
                print('Query: ', query, 'Params: ', params)
                raise e
        # if the query isn't a SELECT, INSERT or UPDATE, we don't need to fetch the results
        if not query.lower().startswith("select") and not query.lower().startswith("insert") and not query.lower().startswith("update"):
            return None
        desc = resp.description
        resp = resp.fetchall()
        # map into a list of dicts for easier access
        if len(resp) == 0:
            return None
        resp = [dict(zip([col[0] for col in desc], row)) for row in resp]
        return resp
    
    def commit(self):
        self.conn.commit()
    
    def close(self):
        if self.conn is not None:
            self.conn.close()
