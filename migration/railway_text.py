import psycopg2

# Your Railway connection string (click "show" to get the full password)
DATABASE_URL = "postgresql://postgres:OzafqpSfkSRdIkTpMNdgtbFvLPvmLYPw@yamabiko.proxy.rlwy.net:58300/railway"

try:
    conn = psycopg2.connect(DATABASE_URL)
    print("✅ Successfully connected to Railway PostgreSQL!")
    
    with conn.cursor() as cur:
        cur.execute("SELECT version();")
        version = cur.fetchone()[0]
        print(f"PostgreSQL version: {version}")
    
    conn.close()
except Exception as e:
    print(f"❌ Connection failed: {e}")