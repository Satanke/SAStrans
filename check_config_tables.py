#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3

def check_config_tables():
    """检查配置相关的表"""
    conn = sqlite3.connect('translation_db.sqlite')
    cursor = conn.cursor()
    
    # 获取所有表名
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    all_tables = [row[0] for row in cursor.fetchall()]
    
    print("所有数据库表:")
    for table in sorted(all_tables):
        print(f"  - {table}")
    
    # 查找配置相关的表
    config_tables = [table for table in all_tables if 'config' in table.lower()]
    
    print(f"\n配置相关的表 ({len(config_tables)}个):")
    for table in config_tables:
        print(f"  - {table}")
        
        # 查看表结构
        cursor.execute(f"PRAGMA table_info({table})")
        columns = cursor.fetchall()
        print(f"    字段: {[col[1] for col in columns]}")
        
        # 查看记录数
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"    记录数: {count}")
        
        if count > 0:
            # 显示前几条记录
            cursor.execute(f"SELECT * FROM {table} LIMIT 3")
            records = cursor.fetchall()
            print(f"    样本数据:")
            for i, record in enumerate(records):
                print(f"      {i+1}. {record}")
        print()
    
    conn.close()

if __name__ == '__main__':
    check_config_tables()