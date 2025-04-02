import mysql.connector
from config import dbconfig_read, dbconfig_edit
from multilingual_search import translate_query
from popular_movies import get_popular_queries
from popular_movies import get_connection


def get_read_connection():
    try:
        connection = mysql.connector.connect(**dbconfig_read)
        return connection
    except mysql.connector.Error as err:
        print(f"Ошибка подключения (чтение): {err}")
        return None


def get_edit_connection():
    try:
        connection = mysql.connector.connect(**dbconfig_edit)
        return connection
    except mysql.connector.Error as err:
        print(f"Ошибка подключения (редактирование): {err}")
        return None


def create_queries_table():
    connection = get_edit_connection()
    if not connection:
        return
    try:
        cursor = connection.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS queries (
                query_id INT AUTO_INCREMENT PRIMARY KEY,
                query_text VARCHAR(255),
                query_type ENUM('keyword', 'genre_year'),
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        connection.commit()
        print("Таблица 'queries' готова в 'ich_edit'.")
    except mysql.connector.Error as err:
        print(f"Ошибка при создании таблицы: {err}")
    finally:
        cursor.close()
        connection.close()


def save_query(query_text, query_type):
    connection = get_edit_connection()
    if not connection:
        return
    try:
        cursor = connection.cursor()
        cursor.execute("USE ich_edit")
        query = """
            INSERT INTO queries (query_text, query_type)
            VALUES (%s, %s)
        """
        cursor.execute(query, (query_text, query_type))
        connection.commit()
    except mysql.connector.Error as err:
        print(f"Ошибка при сохранении запроса: {err}")
    finally:
        cursor.close()
        connection.close()


def search_movies_by_keyword(keyword, target_language='en'):
    connection = get_read_connection()
    if not connection:
        return []
    cursor = None
    try:
        translated_keyword = translate_query(keyword, target_language)
        if not translated_keyword:
            print("Не удалось перевести запрос.")
            return []

        cursor = connection.cursor()
        query = "SELECT film_id, title, description FROM film"
        cursor.execute(query)
        results = cursor.fetchall()
        connection.close()

        if not results:
            print("В базе данных нет данных для обработки.")
            return []

        keywords = [word.lower() for word in translated_keyword.split()]
        filtered_results = [
            (film_id, title) for film_id, title, description in results
            if all(kw in (title + " " + description).lower()
                   for kw in keywords)
        ]

        if not filtered_results:
            print("Фильмы не найдены. Запрос необходимо конкретизировать.")
        return filtered_results

    except mysql.connector.Error as err:
        print(f"Ошибка SQL: {err}")
        return []
    finally:
        if cursor:
            cursor.close()


def search_movies_by_genre_and_year(genre, year):
    connection = get_read_connection()
    if not connection:
        return []
    try:
        cursor = connection.cursor()
        query = """
            SELECT f.film_id, f.title
            FROM film f
            JOIN film_category fc ON f.film_id = fc.film_id
            JOIN category c ON fc.category_id = c.category_id
            WHERE c.name = %s AND f.release_year = %s
            LIMIT 10
        """
        cursor.execute(query, (genre, year))
        results = cursor.fetchall()
        connection.close()
        return results
    except mysql.connector.Error as err:
        print(f"Ошибка SQL: {err}")
        return []
    finally:
        cursor.close()


def get_popular_movies(connection, limit=10):
    try:
        cursor = connection.cursor()
        cursor.execute("USE sakila")
        query = """
        SELECT film_id, title
        FROM film
        ORDER BY release_year DESC
        LIMIT %s
        """
        cursor.execute(query, (limit,))
        return cursor.fetchall()
    except mysql.connector.Error as err:
        print(f"Ошибка SQL: {err}")
        return []
    finally:
        cursor.close()


def get_statistics(connection):
    try:
        cursor = connection.cursor()

        cursor.execute("SELECT COUNT(*) FROM queries")
        total_queries_count = cursor.fetchone()[0]
        print(f"\nОбщее количество запросов: {total_queries_count}")

        cursor.execute("SELECT COUNT(DISTINCT query_text) FROM queries")
        unique_queries_count = cursor.fetchone()[0]
        print(f"Количество уникальных запросов: {unique_queries_count}")

        cursor.execute("""
            SELECT query_text, COUNT(*) AS query_count
            FROM queries
            GROUP BY query_text
            ORDER BY query_count DESC
            LIMIT 5
        """)
        top_queries_results = cursor.fetchall()

        print("\nТоп-5 популярных запросов:")
        if top_queries_results:
            for query_text, query_count in top_queries_results:
                print(f"{query_text}: {query_count}")
        else:
            print("Нет данных по популярным запросам.")

    except mysql.connector.Error as err:
        print(f"Ошибка SQL при сборе статистики: {err}")
    finally:
        cursor.close()


def main():
    create_queries_table()
    while True:
        print("\nДобро пожаловать в систему поиска фильмов!")
        print("1. Найти фильмы по ключевому слову")
        print("2. Найти фильмы по жанру и году")
        print("3. Вывести самые популярные запросы")
        print("4. Показать статистику запросов")
        print("0. Выйти")
        action = input("Выберите действие: ")

        if action == "1":
            keyword = input("Введите ключевое слово для поиска: ")
            results = search_movies_by_keyword(keyword)
            save_query(keyword, "keyword")
            print("\nРезультаты поиска:")
            for film_id, title in results:
                print(f"ID: {film_id}, Название: {title}")

        elif action == "2":
            genre = input("Введите жанр: ")
            year = input("Введите год: ")
            results = search_movies_by_genre_and_year(genre, year)
            query_text = f"{genre}, {year}"
            save_query(query_text, "genre_year")
            print("\nРезультаты поиска:")
            for film_id, title in results:
                print(f"ID: {film_id}, Название: {title}")

        elif action == "3":
            connection = get_edit_connection()
            results = get_popular_queries(connection)
            print("\nПопулярные запросы:")
            if results:
                for query_text, query_count in results:
                    print(f"Запрос: {query_text}, Количество: {query_count}")
            else:
                print("Популярные запросы отсутствуют.")
        elif action == "4":
            connection = get_connection("edit")
            get_statistics(connection)

        else:
            print("Некорректный ввод. Попробуйте снова.")


if __name__ == "__main__":
    main()
