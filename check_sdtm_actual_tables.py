import sqlite3

# 连接数据库
conn = sqlite3.connect('translation_db.sqlite')
cursor = conn.cursor()

# 查找所有SDTM相关表
cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND (name LIKE 'sdtm2dataset%' OR name LIKE 'sdtm2variable%' OR name LIKE 'supp_qnamlist%')")
sdtm_tables = [row[0] for row in cursor.fetchall()]

print("找到的SDTM相关表:")
for table in sdtm_tables:
    print(f"\n=== {table} ===")
    
    # 检查表结构
    cursor.execute(f"PRAGMA table_info({table})")
    columns = cursor.fetchall()
    print(f"列: {[col[1] for col in columns]}")
    
    # 检查记录数
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    count = cursor.fetchone()[0]
    print(f"记录数: {count}")
    
    # 显示前几条记录
    if count > 0:
        cursor.execute(f"SELECT * FROM {table} LIMIT 3")
        rows = cursor.fetchall()
        print("示例数据:")
        for i, row in enumerate(rows, 1):
            print(f"  {i}: {row}")

conn.close()