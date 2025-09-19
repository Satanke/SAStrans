import sqlite3

# 连接数据库
conn = sqlite3.connect('translation_db.sqlite')
cursor = conn.cursor()

# 检查是否存在SDTM相关表
sdtm_tables = ['sdtm2dataset', 'sdtm2variable', 'supp_qnamlist']
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
all_tables = [row[0] for row in cursor.fetchall()]

print("所有表:", all_tables)
print("\n检查SDTM相关表:")
for table in sdtm_tables:
    if table in all_tables:
        print(f"✓ {table} 存在")
        # 检查表结构
        cursor.execute(f"PRAGMA table_info({table})")
        columns = cursor.fetchall()
        print(f"  列: {[col[1] for col in columns]}")
        # 检查记录数
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"  记录数: {count}")
    else:
        print(f"✗ {table} 不存在")

conn.close()