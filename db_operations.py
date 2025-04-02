import mysql.connector


def get_connection(config):
    try:
        connection = mysql.connector.connect(**config)
        return connection
    except mysql.connector.Error as err:
        print(f"Ошибка подключения: {err}")
        return None


def check_user_exist(connection, username):
    try:
        cursor = connection.cursor()
        query = "SELECT id FROM users WHERE username = %s"
        cursor.execute(query, (username,))
        result = cursor.fetchone()
        return result is not None
    except mysql.connector.Error as err:
        print(f"Ошибка SQL: {err}")
        return False
    finally:
        cursor.close()


def add_user_to_database(connection, username, firstname, lastname):
    try:
        cursor = connection.cursor()
        query = "INSERT INTO users (username, firstname, lastname) "
        "VALUES (%s, %s, %s)"
        cursor.execute(query, (username, firstname, lastname))
        connection.commit()
        print(f"Пользователь {username} успешно добавлен.")
    except mysql.connector.Error as err:
        print(f"Ошибка SQL: {err}")
    finally:
        cursor.close()


def fetch_user_info(connection, username):
    try:
        cursor = connection.cursor()
        query = "SELECT firstname, lastname FROM users WHERE username = %s"
        cursor.execute(query, (username,))
        result = cursor.fetchone()
        if result:
            return {"firstname": result[0], "lastname": result[1]}
        else:
            print("Пользователь не найден.")
            return None
    except mysql.connector.Error as err:
        print(f"Ошибка SQL: {err}")
        return None
    finally:
        cursor.close()


def change_user_info(connection, username, field, new_value):
    try:
        cursor = connection.cursor()
        query = f"UPDATE users SET {field} = %s WHERE username = %s"
        cursor.execute(query, (new_value, username))
        connection.commit()
        print(f"Информация о пользователе {username} успешно обновлена.")
    except mysql.connector.Error as err:
        print(f"Ошибка SQL: {err}")
    finally:
        cursor.close()
