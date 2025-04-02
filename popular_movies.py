from config import dbconfig_read, dbconfig_edit
import mysql.connector


def get_connection(mode="edit"):
    """
    Создаёт соединение с базой данных.
    :param mode: режим подключения ("read" или "edit").
    :return: объект соединения или None в случае ошибки.
    """
    try:
        if mode == "read":
            db_config = dbconfig_read
        elif mode == "edit":
            db_config = dbconfig_edit
        else:
            raise ValueError("Некорректный режим подключения. "
                             "Используйте 'read' или 'edit'.")

        connection = mysql.connector.connect(**db_config)
        return connection
    except mysql.connector.Error as err:
        print(f"Ошибка подключения: {err}")
        return None


def save_query(connection, film_id, query_text):
    try:
        cursor = connection.cursor()
        query = "INSERT INTO queries (film_id, query_text) VALUES (%s, %s)"
        cursor.execute(query, (film_id, query_text))
        connection.commit()
    except mysql.connector.Error as err:
        print(f"Ошибка сохранения запроса: {err}")
    finally:
        cursor.close()


def get_popular_queries(connection, limit=10):
    try:
        cursor = connection.cursor()
        query = """
        SELECT query_text, COUNT(*) AS query_count
        FROM queries
        GROUP BY query_text
        ORDER BY query_count DESC
        LIMIT %s
        """
        cursor.execute(query, (limit,))
        return cursor.fetchall()
    except mysql.connector.Error as err:
        print(f"Ошибка SQL: {err}")
        return []
    finally:
        cursor.close()
