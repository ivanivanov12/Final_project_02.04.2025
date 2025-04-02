from googletrans import Translator
from config import dbconfig_edit
from nltk.corpus import stopwords
import nltk
import mysql.connector

nltk.download('stopwords', quiet=True)


def translate_query(query, target_language='en'):
    translator = Translator()
    for _ in range(3):
        try:
            translated = translator.translate(query, dest=target_language)
            if translated and translated.text:
                return translated.text
        except Exception as e:
            print(f"Ошибка перевода, повторная попытка: {e}")
    print(f"Не удалось перевести запрос '{query}' после нескольких попыток.")
    return None


def multilingual_search(connection, query, target_language='en'):
    translated_query = translate_query(query, target_language)
    if not translated_query:
        print("Не удалось перевести запрос.")
        return []
    english_stop_words = set(stopwords.words('english'))
    russian_stop_words = set(stopwords.words('russian'))
    combined_stop_words = english_stop_words.union(russian_stop_words)
    query_words = [
        word.lower() for word in translated_query.split()
        if word.lower() not in combined_stop_words
    ]

    if not query_words:
        print("Запрос слишком общий. Уточните ключевые слова.")
        return []

    try:
        cursor = connection.cursor()
        query_string = """
            SELECT film_id, title, description
            FROM film
        """
        cursor.execute(query_string)
        results = cursor.fetchall()

        filtered_results = []
        for film_id, title, description in results:
            combined_text = f"{title} {description}".lower()
            words = combined_text.split()
            if all(word in words for word in query_words):
                filtered_results.append((film_id, title))
        return filtered_results
    except mysql.connector.Error as err:
        print(f"Ошибка SQL: {err}")
        return []
    finally:
        cursor.close()


if __name__ == "__main__":
    from db_operations import get_connection

    connection = get_connection(dbconfig_edit)

    if connection:
        user_query = input("Введите поисковый запрос: ")
        films = multilingual_search(
            connection, user_query, target_language='en'
        )

        print("\nРезультаты поиска:")
        for film_id, title in films:
            print(f"ID: {film_id}, Название: {title}")

        connection.close()
