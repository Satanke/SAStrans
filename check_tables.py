#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sqlite3

def check_database_tables():
    """检查数据库中的表结构"""
    try:
        conn = sqlite3.connect('translation_db.sqlite')
        cursor = conn.cursor()
        
        # 获取所有表名
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        print('=== 所有数据库表 ===')
        all_tables = [table[0] for table in tables]
        
        # 查找合并表
        merged_tables = [t for t in all_tables if 'merged' in t.lower()]
        print(f'\n合并表 ({len(merged_tables)}个):')
        for table in merged_tables:
            print(f'  - {table}')
            cursor.execute(f"PRAGMA table_info({table})")
            columns = cursor.fetchall()
            if columns:
                print(f'    字段: {[col[1] for col in columns]}')
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f'    记录数: {count}')
        
        # 查找SDTM相关表
        sdtm_tables = [t for t in all_tables if t.lower().startswith('sdtm')]
        print(f'\nSDTM相关表 ({len(sdtm_tables)}个):')
        for table in sdtm_tables:
            print(f'  - {table}')
            cursor.execute(f"PRAGMA table_info({table})")
            columns = cursor.fetchall()
            if columns:
                print(f'    字段: {[col[1] for col in columns]}')
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f'    记录数: {count}')
        
        # 查找SUPP相关表
        supp_tables = [t for t in all_tables if 'supp' in t.lower()]
        print(f'\nSUPP相关表 ({len(supp_tables)}个):')
        for table in supp_tables:
            print(f'  - {table}')
            cursor.execute(f"PRAGMA table_info({table})")
            columns = cursor.fetchall()
            if columns:
                print(f'    字段: {[col[1] for col in columns]}')
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f'    记录数: {count}')
        
        # 显示MedDRA和WHODrug版本信息
        print('\n=== MedDRA版本信息 ===')
        cursor.execute("SELECT DISTINCT version FROM meddra_tables WHERE version IS NOT NULL ORDER BY version")
        meddra_versions = cursor.fetchall()
        for version in meddra_versions:
            print(f'  - {version[0]}')
        
        print('\n=== WHODrug版本信息 ===')
        cursor.execute("SELECT DISTINCT version FROM whodrug_tables WHERE version IS NOT NULL ORDER BY version")
        whodrug_versions = cursor.fetchall()
        for version in whodrug_versions:
            print(f'  - {version[0]}')
        
        conn.close()
        
    except Exception as e:
        print(f'错误: {e}')

if __name__ == '__main__':
    check_database_tables()