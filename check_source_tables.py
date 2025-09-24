import sqlite3

conn = sqlite3.connect('translation_db.sqlite')
cursor = conn.cursor()

# 检查所有表
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
all_tables = cursor.fetchall()
print('数据库中所有的表:')
for table in all_tables:
    print(f'  {table[0]}')

print('\n' + '='*50)

# 检查指定的6张源表
target_tables = [
    'sdtm2dataset3_2cn', 'sdtm2dataset3_2en', 
    'sdtm2dataset3_3cn', 'sdtm2dataset3_3en',
    'sdtm2dataset3_4cn', 'sdtm2dataset3_4en'
]

print('检查目标源表:')
for table in target_tables:
    cursor.execute('SELECT name FROM sqlite_master WHERE type="table" AND name=?', (table,))
    exists = cursor.fetchone()
    if exists:
        cursor.execute(f'SELECT COUNT(*) FROM {table}')
        count = cursor.fetchone()[0]
        print(f'{table}: 存在，记录数: {count}')
        
        # 查看表结构
        cursor.execute(f'PRAGMA table_info({table})')
        columns = cursor.fetchall()
        print(f'  列: {[col[1] for col in columns]}')
        
        # 查看前2条数据
        if count > 0:
            cursor.execute(f'SELECT * FROM {table} LIMIT 2')
            rows = cursor.fetchall()
            print(f'  前2条数据: {rows}')
    else:
        print(f'{table}: 不存在')

conn.close()