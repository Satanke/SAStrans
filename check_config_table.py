#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3

def check_config_table():
    """检查配置表"""
    conn = sqlite3.connect('translation_db.sqlite')
    cursor = conn.cursor()
    
    # 获取所有表名
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    all_tables = [row[0] for row in cursor.fetchall()]
    
    print("所有表:")
    for table in sorted(all_tables):
        print(f"  - {table}")
    
    # 查找配置相关的表
    config_tables = [table for table in all_tables if 'config' in table.lower()]
    
    print(f"\n配置相关的表:")
    for table in config_tables:
        print(f"  - {table}")
    
    # 检查是否有translation_library_config表
    if 'translation_library_config' in all_tables:
        print("\n✅ translation_library_config表存在")
        
        # 查看表结构
        cursor.execute("PRAGMA table_info(translation_library_config)")
        columns = cursor.fetchall()
        print("表结构:")
        for col in columns:
            print(f"  {col[1]} ({col[2]})")
        
        # 查看记录数
        cursor.execute("SELECT COUNT(*) FROM translation_library_config")
        count = cursor.fetchone()[0]
        print(f"记录数: {count}")
        
        if count > 0:
            cursor.execute("SELECT * FROM translation_library_config LIMIT 3")
            records = cursor.fetchall()
            print("样本数据:")
            for i, record in enumerate(records):
                print(f"  {i+1}. {record}")
    else:
        print("\n❌ translation_library_config表不存在")
        
        # 检查是否有其他可能的配置表
        possible_names = ['config', 'library_config', 'translation_config']
        for name in possible_names:
            matching_tables = [table for table in all_tables if name in table.lower()]
            if matching_tables:
                print(f"\n可能的配置表 (包含'{name}'):")
                for table in matching_tables:
                    print(f"  - {table}")
    
    conn.close()

if __name__ == '__main__':
    check_config_table()