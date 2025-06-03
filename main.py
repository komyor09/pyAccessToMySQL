import time
import logging
from config import DBConfig
import pyodbc
import pymysql
from pymysql.err import OperationalError

# Настройка логирования
logging.basicConfig(
    filename="access_mysql_sync.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


class AccessToMySQLSync:

    def __init__(self):
        self.config = DBConfig()
        # Проверка на несовпадение длины
        if len(self.config.ACCESS_SELECTED_FIELDS) != len(self.config.FIELD_MAPPING):
            raise ValueError(
                "Количество полей Access и их маппинг в MySQL не совпадает."
            )

    def get_mysql_connection(self):
        return pymysql.connect(
            host=self.config.MYSQL_HOST,
            port=self.config.MYSQL_PORT,
            user=self.config.MYSQL_USER,
            password=self.config.MYSQL_PASS,
            database=self.config.MYSQL_DB,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
        )
    
    def ensure_table_and_columns(self, mysql_conn):
        # Проверяем существование таблицы
        with mysql_conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            """, (self.config.MYSQL_DB, 'access_logs'))
            if cur.fetchone()['COUNT(*)'] == 0:
                # Таблицы нет — создаём
                logging.info("[MySQL] Таблица access_logs не найдена, создаём...")
                columns_sql = []
                for access_field, mysql_field in self.config.FIELD_MAPPING.items():
                    # Пример типа — подгони под свои данные
                    col_type = "VARCHAR(255)"  # по умолчанию
                    if mysql_field == "raw_id":
                        col_type = "INT PRIMARY KEY"
                    elif "date" in mysql_field:
                        col_type = "DATETIME"
                    elif "id" in mysql_field:
                        col_type = "INT"
                    elif mysql_field == "in_out":
                        col_type = "TINYINT(1)"
                    columns_sql.append(f"{mysql_field} {col_type}")
                create_sql = f"CREATE TABLE access_logs ({', '.join(columns_sql)});"
                cur.execute(create_sql)
                mysql_conn.commit()
                logging.info("[MySQL] Таблица access_logs создана.")
            else:
                # Таблица есть — проверяем столбцы
                logging.info("[MySQL] Таблица access_logs найдена, проверяем столбцы...")
                cur.execute(f"SHOW COLUMNS FROM access_logs;")
                existing_columns = {row['Field'] for row in cur.fetchall()}
                for access_field, mysql_field in self.config.FIELD_MAPPING.items():
                    if mysql_field not in existing_columns:
                        logging.info(f"[MySQL] Добавляем столбец {mysql_field}...")
                        # Типы столбцов можно сделать как в создании таблицы
                        col_type = "VARCHAR(255)"
                        if mysql_field == "raw_id":
                            col_type = "INT"
                        elif "date" in mysql_field:
                            col_type = "DATETIME"
                        elif "id" in mysql_field:
                            col_type = "INT"
                        elif mysql_field == "in_out":
                            col_type = "TINYINT(1)"
                        alter_sql = f"ALTER TABLE access_logs ADD COLUMN {mysql_field} {col_type};"
                        cur.execute(alter_sql)
                        mysql_conn.commit()
                logging.info("[MySQL] Проверка столбцов завершена.")

    def get_last_raw_id(self, mysql_conn):
        with mysql_conn.cursor() as cur:
            cur.execute("SELECT COALESCE(MAX(raw_id), 0) AS last_id FROM access_logs;")
            row = cur.fetchone()
            return row["last_id"] if row else 0

    def fetch_new_rows_from_access(self, last_id):
        try:
            conn = pyodbc.connect(self.config.ACCESS_CONN_STR, timeout=1)
        except pyodbc.Error as e:
            if "Not a valid password" in str(e):
                logging.error("[Access] Неверный пароль к базе данных.")
            else:
                logging.warning(f"[Access] не удалось подключиться: {e}")
            return []

        cursor = conn.cursor()
        selected_fields = self.config.ACCESS_SELECTED_FIELDS
        query = f"SELECT {', '.join(selected_fields)} FROM {self.config.ACCESS_TABLE_NAME} WHERE f_RecID > ? ORDER BY f_RecID ASC"
        try:
            cursor.execute(query, last_id)
            rows = cursor.fetchall()
        except Exception as e:
            logging.error(f"[Access] ошибка при SELECT: {e}")
            rows = []
        finally:
            cursor.close()
            conn.close()

        return rows

    def insert_rows_to_mysql(self, mysql_conn, rows):
        if not rows:
            return 0

        field_mapping = self.config.FIELD_MAPPING
        mysql_fields = [
            field_mapping[access_field]
            for access_field in self.config.ACCESS_SELECTED_FIELDS
        ]
        placeholders = ", ".join(["%s"] * len(mysql_fields))
        columns = ", ".join(mysql_fields)

        sql = f"""
            INSERT INTO access_logs ({columns})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {mysql_fields[0]} = {mysql_fields[0]};
        """

        inserted = 0
        with mysql_conn.cursor() as cur:
            for row in rows:
                values = [
                    getattr(row, access_field)
                    for access_field in self.config.ACCESS_SELECTED_FIELDS
                ]
                try:
                    cur.execute(sql, values)
                    inserted += cur.rowcount
                except Exception as e:
                    logging.error(
                        f"[MySQL] Ошибка при вставке raw_id={getattr(row, 'f_RecID', '?')}: {e}"
                    )

            mysql_conn.commit()

        return inserted

    def main_loop(self):
        try:
            mysql_conn = self.get_mysql_connection()
            self.ensure_table_and_columns(mysql_conn)
        except OperationalError as e:
            logging.critical(f"[MySQL] не удалось подключиться: {e}")
            return

        logging.info("Запуск цикла опроса Access → MySQL")
        while True:
            try:
                last_id = self.get_last_raw_id(mysql_conn)
                rows = self.fetch_new_rows_from_access(last_id)
                if rows:
                    cnt = self.insert_rows_to_mysql(mysql_conn, rows)
                    if cnt:
                        logging.info(
                            f"[OK] Вставлено {cnt} записей до f_RecID={rows[-1].f_RecID}"
                        )
                    else:
                        logging.info(
                            f"[Info] Обнаружено {len(rows)} строк, но они уже существуют."
                        )
                time.sleep(self.config.POLL_INTERVAL)

            except Exception as e:
                logging.error(f"[Loop] Ошибка: {e}")
                try:
                    mysql_conn.close()
                except:
                    pass
                time.sleep(5)
                try:
                    mysql_conn = self.get_mysql_connection()
                except OperationalError as e2:
                    logging.critical(f"[MySQL] Повторное подключение провалено: {e2}")
                    time.sleep(self.config.POLL_INTERVAL)


if __name__ == "__main__":
    AccessToMySQLSync().main_loop()
