#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
import pandas as pd

def check_metadata_table():
    """检查metadata表的内容"""
    try:
        conn = sqlite3.connect('translation_db.sqlite')
        cursor = conn.cursor()
        
        # 获取表结构
        cursor.execute('PRAGMA table_info(metadata)')
        columns_info = cursor.fetchall()
        columns = [col[1] for col in columns_info]
        print('metadata表字段:')
        for col_info in columns_info:
            print(f'  - {col_info[1]} ({col_info[2]})')
        
        # 获取总记录数
        cursor.execute('SELECT COUNT(*) FROM metadata')
        total_count = cursor.fetchone()[0]
        print(f'\nmetadata表总记录数: {total_count}')
        
        # 获取样本数据
        cursor.execute('SELECT * FROM metadata LIMIT 10')
        rows = cursor.fetchall()
        
        print('\nmetadata表样本数据:')
        for i, row in enumerate(rows, 1):
            print(f'\n记录 {i}:')
            for j, col in enumerate(columns):
                value = row[j] if j < len(row) else None
                print(f'  {col}: {value}')
        
        # 检查__TEST_CN和__TEST_EN的数据分布
        cursor.execute('SELECT COUNT(*) FROM metadata WHERE __TEST_CN IS NOT NULL AND __TEST_CN != ""')
        cn_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM metadata WHERE __TEST_EN IS NOT NULL AND __TEST_EN != ""')
        en_count = cursor.fetchone()[0]
        
        print(f'\n数据分布:')
        print(f'  有中文翻译的记录: {cn_count}')
        print(f'  有英文翻译的记录: {en_count}')
        
        # 检查DOMAIN分布
        cursor.execute('SELECT DOMAIN, COUNT(*) FROM metadata GROUP BY DOMAIN ORDER BY COUNT(*) DESC')
        domain_stats = cursor.fetchall()
        print(f'\nDOMAIN分布:')
        for domain, count in domain_stats:
            print(f'  {domain}: {count}条记录')
        
        # 检查一些具体的翻译示例
        cursor.execute('SELECT ORG_TEST, __TEST_CN, __TEST_EN FROM metadata WHERE __TEST_CN IS NOT NULL AND __TEST_EN IS NOT NULL LIMIT 5')
        translation_examples = cursor.fetchall()
        print(f'\n翻译示例:')
        for org_test, test_cn, test_en in translation_examples:
            print(f'  原始值: {org_test}')
            print(f'  中文: {test_cn}')
            print(f'  英文: {test_en}')
            print()
        
        conn.close()
        
    except Exception as e:
        print(f'检查metadata表时出错: {e}')

if __name__ == '__main__':
    check_metadata_table()