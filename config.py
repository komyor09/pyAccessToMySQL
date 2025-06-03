from pathlib import Path
from dotenv import load_dotenv
import os
import json


load_dotenv()

class DBConfig:
    # === Конфигурация Access ===
    ACCESS_FILE_PATH = Path(os.getenv("ACCESS_FILE_PATH"))

    if not ACCESS_FILE_PATH.exists():
        raise FileNotFoundError(f"Файл Access не найден: {ACCESS_FILE_PATH}")

    ACCESS_CONN_STR = (
        rf"Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};"
        rf"Dbq={ACCESS_FILE_PATH};"
        rf"PWD={os.getenv('ACCESS_DB_PASSWORD', '')};"
    )


    # === Конфигурация MySQL ===
    MYSQL_HOST = os.getenv("MYSQL_HOST")
    MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
    MYSQL_USER = os.getenv("MYSQL_USER")
    MYSQL_PASS = os.getenv("MYSQL_PASS")
    MYSQL_DB = os.getenv("MYSQL_DB")
    MYSQL_TABLE_NAME = os.getenv("MYSQL_TABLE_NAME")

    # === Конфигурация полей и маппинга ===
    ACCESS_TABLE_NAME = os.getenv("ACCESS_TABLE_NAME", "t_d_SwipeRecord")
    ACCESS_SELECTED_FIELDS = json.loads(os.getenv("ACCESS_SELECTED_FIELDS", "[]"))
    FIELD_MAPPING = json.loads(os.getenv("FIELD_MAPPING", "{}"))


    # === Интервал опроса (в секундах) ===
    POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", 10))

    # === Путь к лог-файлу ===
    LOG_FILE = Path(os.getenv("LOG_FILE", "sync_log.log"))