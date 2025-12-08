import pymssql
from dotenv import load_dotenv
import os
from pathlib import Path

# Load environment variables. Assumes .env is in the same directory or adjust path as needed.
# For the PoC, looking in current directory is safer or hardcoding a path if known.
# Original code looked in C:/Luzzi/Installer/.env, let's look in current dir for PoC simplicity
load_dotenv() 

DB_SERVER = os.getenv('DB_SERVER')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_TIMEOUT = int(os.getenv('DB_TIMEOUT', 30))

def get_db_connection():
    """Establece una conexión a la base de datos usando las credenciales del .env."""
    try:
        conn = pymssql.connect(
            server=DB_SERVER,
            user=DB_USER,
            password=DB_PASSWORD,
            timeout=DB_TIMEOUT
        )
        return conn
    except Exception as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None

def execute_query(query, params=None):
    """Ejecuta una consulta SQL genérica y devuelve los resultados como diccionarios."""
    conn = get_db_connection()
    if conn is None:
        return None
    try:
        cursor = conn.cursor(as_dict=True)
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        results = cursor.fetchall()
        conn.close()
        return results
    except Exception as e:
        print(f"Error al ejecutar la consulta: {e}")
        conn.close()
        return None
