#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3

def check_database_structure():
    """检查数据库结构，查找metadata表和__TEST_CN、__TEST_EN字段"""
    try:
        conn = sqlite3.connect('translation_db.sqlite')
        cursor = conn.cursor()
        
        # 获取所有表名
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        print(f'数据库中共有 {len(tables)} 个表:')
        for table in tables:
            print(f'  - {table}')
        
        # 查找包含metadata的表
        metadata_tables = [t for t in tables if 'metadata' in t.lower()]
        print(f'\n包含metadata的表 ({len(metadata_tables)}个):')
        for table in metadata_tables:
            print(f'  - {table}')
            cursor.execute(f'PRAGMA table_info({table})')
            columns = cursor.fetchall()
            print(f'    字段: {[col[1] for col in columns]}')
        
        # 查找包含__TEST_CN或__TEST_EN字段的表
        print('\n查找包含__TEST_CN或__TEST_EN字段的表:')
        found_test_tables = []
        for table in tables:
            cursor.execute(f'PRAGMA table_info({table})')
            columns = cursor.fetchall()
            column_names = [col[1] for col in columns]
            
            if '__TEST_CN' in column_names or '__TEST_EN' in column_names:
                found_test_tables.append(table)
                print(f'  ✅ {table}: {column_names}')
        
        if not found_test_tables:
            print('  ❌ 未找到包含__TEST_CN或__TEST_EN字段的表')
        
        # 检查所有表的字段，看是否有类似的测试字段
        print('\n查找包含TEST相关字段的表:')
        for table in tables:
            cursor.execute(f'PRAGMA table_info({table})')
            columns = cursor.fetchall()
            column_names = [col[1] for col in columns]
            
            test_columns = [col for col in column_names if 'test' in col.lower()]
            if test_columns:
                print(f'  - {table}: {test_columns}')
        
        conn.close()
        
    except Exception as e:
        print(f'检查数据库结构时出错: {e}')

if __name__ == '__main__':
    check_database_structure()