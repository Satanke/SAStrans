import sqlite3
import requests
import json

def test_version_apis():
    """测试版本API是否正常工作"""
    print("=== 测试版本API ===")
    
    # 测试MedDRA版本API
    try:
        response = requests.get('http://127.0.0.1:5000/api/get_meddra_versions')
        if response.status_code == 200:
            data = response.json()
            print(f"MedDRA版本数量: {len(data.get('versions', []))}")
            if data.get('versions'):
                print(f"MedDRA版本示例: {data['versions'][:3]}")
        else:
            print(f"MedDRA版本API失败: {response.status_code}")
    except Exception as e:
        print(f"MedDRA版本API错误: {e}")
    
    # 测试WHODrug版本API
    try:
        response = requests.get('http://127.0.0.1:5000/api/get_whodrug_versions')
        if response.status_code == 200:
            data = response.json()
            print(f"WHODrug版本数量: {len(data.get('versions', []))}")
            if data.get('versions'):
                print(f"WHODrug版本示例: {data['versions'][:3]}")
        else:
            print(f"WHODrug版本API失败: {response.status_code}")
    except Exception as e:
        print(f"WHODrug版本API错误: {e}")

def test_database_version_queries():
    """测试数据库版本查询是否正常"""
    print("\n=== 测试数据库版本查询 ===")
    
    conn = sqlite3.connect('translation_db.sqlite')
    cursor = conn.cursor()
    
    # 测试MedDRA版本查询
    try:
        cursor.execute('SELECT DISTINCT version FROM meddra_merged WHERE version IS NOT NULL ORDER BY version')
        meddra_versions = cursor.fetchall()
        print(f"数据库中MedDRA版本数量: {len(meddra_versions)}")
        print(f"MedDRA版本列表: {[v[0] for v in meddra_versions[:5]]}")
        
        # 测试特定版本的数据查询
        if meddra_versions:
            test_version = meddra_versions[0][0]
            cursor.execute('SELECT COUNT(*) FROM meddra_merged WHERE version = ?', (test_version,))
            count = cursor.fetchone()[0]
            print(f"版本 {test_version} 的MedDRA记录数: {count}")
            
            # 测试翻译查询
            cursor.execute('SELECT code, name_cn, name_en FROM meddra_merged WHERE version = ? LIMIT 3', (test_version,))
            sample_data = cursor.fetchall()
            print(f"版本 {test_version} 的MedDRA数据示例: {sample_data}")
    except Exception as e:
        print(f"MedDRA版本查询错误: {e}")
    
    # 测试WHODrug版本查询
    try:
        cursor.execute('SELECT DISTINCT version FROM whodrug_merged WHERE version IS NOT NULL ORDER BY version')
        whodrug_versions = cursor.fetchall()
        print(f"\n数据库中WHODrug版本数量: {len(whodrug_versions)}")
        print(f"WHODrug版本列表: {[v[0] for v in whodrug_versions[:5]]}")
        
        # 测试特定版本的数据查询
        if whodrug_versions:
            test_version = whodrug_versions[0][0]
            cursor.execute('SELECT COUNT(*) FROM whodrug_merged WHERE version = ?', (test_version,))
            count = cursor.fetchone()[0]
            print(f"版本 {test_version} 的WHODrug记录数: {count}")
            
            # 测试翻译查询
            cursor.execute('SELECT code, name_cn, name_en FROM whodrug_merged WHERE version = ? LIMIT 3', (test_version,))
            sample_data = cursor.fetchall()
            print(f"版本 {test_version} 的WHODrug数据示例: {sample_data}")
    except Exception as e:
        print(f"WHODrug版本查询错误: {e}")
    
    conn.close()

def test_path_hash_storage():
    """测试路径哈希存储机制"""
    print("\n=== 测试路径哈希存储机制 ===")
    
    conn = sqlite3.connect('translation_db.sqlite')
    cursor = conn.cursor()
    
    # 检查配置表
    tables_to_check = ['mapping_configs', 'translation_library_configs', 'merge_configs']
    
    for table in tables_to_check:
        try:
            cursor.execute(f'SELECT COUNT(*) FROM {table}')
            count = cursor.fetchone()[0]
            print(f"{table} 表记录数: {count}")
            
            if count > 0:
                cursor.execute(f'SELECT path_hash, path FROM {table} LIMIT 3')
                samples = cursor.fetchall()
                print(f"{table} 示例数据: {samples}")
        except Exception as e:
            print(f"检查 {table} 表错误: {e}")
    
    conn.close()

if __name__ == '__main__':
    test_database_version_queries()
    test_version_apis()
    test_path_hash_storage()