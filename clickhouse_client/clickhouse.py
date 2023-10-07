import clickhouse_connect
import threading

class ClickhouseClient:

    def __init__(self, host='10.41.49.3', port=8123, username='default', password='123456') -> None:
        self.client = clickhouse_connect.get_client(host=host, port=port, username=username, password=password)
        # self.client = clickhouse_connect.get_client(host='10.41.49.3', port= 8123, username='linuxadmin', password='$4UhTbsGf2udQ@')
        self.query_lock = threading.Lock()  # Create a lock for query execution

    def get_client(self):
        return self.client

    def get_table_schema(self, table):
        return list(
            map(lambda x: x[0], self.client.query(f"DESCRIBE {table}").result_rows)
        )

    def execute_clickhouse_query(self, query):
        # Acquire the lock to ensure thread safety
        with self.query_lock:
            # Execute the query using the ClickHouse client
            query_result = self.client.query(query).result_set
        # Return the query result
        return query_result
