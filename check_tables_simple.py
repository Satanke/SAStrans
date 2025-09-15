#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3

def check_tables():
    try:
        conn = sqlite3.connect('translation_database.db')
        cursor = conn.cursor()
        
        # 查找所有表
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        all_tables = cursor.fetchall()
        print(f"数据库中所有表 ({len(all_tables)}个):")
        
        meddra_tables = []
        whodrug_tables = []
        other_tables = []
        
        for table in all_tables:
            table_name = table[0]
            if 'meddra' in table_name.lower():
                meddra_tables.append(table_name)
            elif 'whodrug' in table_name.lower():
                whodrug_tables.append(table_name)
            else:
                other_tables.append(table_name)
        
        print(f"\nMedDRA表 ({len(meddra_tables)}个):")
        for table in meddra_tables[:10]:  # 只显示前10个
            print(f"  - {table}")
        if len(meddra_tables) > 10:
            print(f"  ... 还有{len(meddra_tables)-10}个表")
        
        print(f"\nWHODrug表 ({len(whodrug_tables)}个):")
        for table in whodrug_tables[:5]:  # 只显示前5个
            print(f"  - {table}")
        if len(whodrug_tables) > 5:
            print(f"  ... 还有{len(whodrug_tables)-5}个表")
        
        print(f"\n其他表 ({len(other_tables)}个):")
        for table in other_tables:
            print(f"  - {table}")
        
        # 检查是否有merged表
        merged_tables = [t for t in all_tables if 'merged' in t[0].lower()]
        if merged_tables:
            print(f"\nMerged表:")
            for table in merged_tables:
                print(f"  - {table[0]}")
                
                # 查看表结构
                cursor.execute(f"PRAGMA table_info({table[0]})")
                columns = cursor.fetchall()
                print(f"    列: {[col[1] for col in columns]}")
                
                # 查看记录数
                cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
                count = cursor.fetchone()[0]
                print(f"    记录数: {count}")
        
        conn.close()
        
    except Exception as e:
        print(f"错误: {e}")

if __name__ == '__main__':
    check_tables()