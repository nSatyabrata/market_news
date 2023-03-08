import psycopg2


class DatabaseConnectionError(Exception):
    '''Custom exception for database connection error.'''

    def __init__(self, message) -> None:
        self.message = message
        super().__init__(self.message)

class Database:

    def __init__(self, host: str, database: str, user: str, password: str, port: int=5432) -> None:
        try:
            self._connection = psycopg2.connect(
                host=host,
                database=database,
                user=user,
                password=password,
                port=port
            )

            self._cursor = self._connection.cursor()
        except Exception as error:
            raise DatabaseConnectionError(f"Unable to connect to database.\n{error=}")

    def query(self, query: str):
        self._cursor.execute(query)

    def commit(self):
        self._connection.commit()

    def close(self):
        self._cursor.close()
        self._connection.close()
    