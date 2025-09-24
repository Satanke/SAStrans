import sqlite3

def check_database_structure():
    conn = sqlite3.connect('translation_db.sqlite')
    cursor = conn.cursor()
    
    print('=== whodrug_merged表结构 ===')
    cursor.execute('PRAGMA table_info(whodrug_merged)')
    columns = cursor.fetchall()
    for col in columns:
        print(f'  {col[1]} ({col[2]})')
    
    print('\n=== meddra_merged表结构 ===')
    cursor.execute('PRAGMA table_info(meddra_merged)')
    columns = cursor.fetchall()
    for col in columns:
        print(f'  {col[1]} ({col[2]})')
    
    print('\n=== 检查whodrug_merged中level字段的数据 ===')
    cursor.execute('SELECT DISTINCT level FROM whodrug_merged WHERE level IS NOT NULL LIMIT 10')
    levels = cursor.fetchall()
    if levels:
        for level in levels:
            print(f'  Level: {level[0]}')
    else:
        print('  无level数据')
    
    print('\n=== 检查whodrug_merged样本数据 ===')
    cursor.execute('SELECT code, name_cn, name_en, version, level FROM whodrug_merged LIMIT 3')
    samples = cursor.fetchall()
    for sample in samples:
        print(f'  Code: {sample[0]}, CN: {sample[1][:30]}..., EN: {sample[2][:30]}..., Version: {sample[3]}, Level: {sample[4]}')
    
    conn.close()

if __name__ == '__main__':
    check_database_structure()