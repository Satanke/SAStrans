import sqlite3

def check_merged_tables():
    conn = sqlite3.connect('translation_db.sqlite')
    cursor = conn.cursor()
    
    # 检查所有表
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    all_tables = cursor.fetchall()
    print('所有表:', [table[0] for table in all_tables])
    
    # 检查merged表
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%merged%'")
    merged_tables = cursor.fetchall()
    print('Merged表:', [table[0] for table in merged_tables])
    
    # 检查meddra_merged表结构
    try:
        cursor.execute('PRAGMA table_info(meddra_merged)')
        meddra_structure = cursor.fetchall()
        print('meddra_merged表结构:', meddra_structure)
        
        # 检查version字段的值
        cursor.execute('SELECT DISTINCT version FROM meddra_merged LIMIT 10')
        meddra_versions = cursor.fetchall()
        print('meddra_merged版本示例:', meddra_versions)
    except Exception as e:
        print('meddra_merged表不存在或查询失败:', e)
    
    # 检查whodrug_merged表结构
    try:
        cursor.execute('PRAGMA table_info(whodrug_merged)')
        whodrug_structure = cursor.fetchall()
        print('whodrug_merged表结构:', whodrug_structure)
        
        # 检查version字段的值
        cursor.execute('SELECT DISTINCT version FROM whodrug_merged LIMIT 10')
        whodrug_versions = cursor.fetchall()
        print('whodrug_merged版本示例:', whodrug_versions)
    except Exception as e:
        print('whodrug_merged表不存在或查询失败:', e)
    
    conn.close()

if __name__ == '__main__':
    check_merged_tables()