import psycopg2

def test_db_connection():
    try:
        conn = psycopg2.connect(
            dbname='house_predictions',
            user='postgres',
            password='postgres',
            host='192.168.56.135',
            port='5432'
        )
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        conn.close()
    except Exception as e:
        assert False, f"Database connection failed: {e}"