import sqlite3

conn = sqlite3.connect('translation_db.sqlite')
cursor = conn.cursor()

# 检查所有表
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = cursor.fetchall()
print('数据库中的表:')
for table in tables:
    print(f'  {table[0]}')

# 检查datalabel_mergeds表是否存在
cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='datalabel_mergeds'")
if cursor.fetchone():
    print('\ndatalabel_mergeds表存在')
    cursor.execute('SELECT COUNT(*) FROM datalabel_mergeds')
    count = cursor.fetchone()[0]
    print(f'记录数: {count}')
    
    if count > 0:
        cursor.execute('SELECT dataset, name_cn, name_en, version FROM datalabel_mergeds LIMIT 5')
        rows = cursor.fetchall()
        print('\n前5条记录:')
        for row in rows:
            print(f'  数据集: {row[0]}, 中文: {row[1]}, 英文: {row[2]}, 版本: {row[3]}')
else:
    print('\ndatalabel_mergeds表不存在')

conn.close()