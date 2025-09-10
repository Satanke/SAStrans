#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
import json

def check_translation_configs():
    """检查翻译配置表"""
    conn = sqlite3.connect('translation_db.sqlite')
    cursor = conn.cursor()
    
    print("=== 检查translation_library_configs表 ===")
    
    # 查看表结构
    cursor.execute("PRAGMA table_info(translation_library_configs)")
    columns = cursor.fetchall()
    print("\n表结构:")
    for col in columns:
        print(f"  {col[1]} ({col[2]}) - {col[5] if col[5] else ''}")
    
    # 查看记录数
    cursor.execute("SELECT COUNT(*) FROM translation_library_configs")
    count = cursor.fetchone()[0]
    print(f"\n记录数: {count}")
    
    if count > 0:
        # 显示所有记录
        cursor.execute("SELECT * FROM translation_library_configs")
        records = cursor.fetchall()
        
        print("\n所有配置记录:")
        for i, record in enumerate(records):
            print(f"\n记录 {i+1}:")
            print(f"  ID: {record[0]}")
            print(f"  项目路径: {record[1]}")
            print(f"  MedDRA版本: {record[2]}")
            print(f"  WHODrug版本: {record[3]}")
            print(f"  IG版本: {record[4]}")
            
            # 解析MedDRA配置
            if record[5]:
                try:
                    meddra_config = json.loads(record[5])
                    print(f"  MedDRA配置项数: {len(meddra_config)}")
                    if meddra_config:
                        print("  MedDRA配置样本:")
                        for j, item in enumerate(meddra_config[:3]):
                            print(f"    {j+1}. name: {item.get('name_column')}, code: {item.get('code_column')}")
                except json.JSONDecodeError:
                    print(f"  MedDRA配置: JSON解析失败")
            else:
                print(f"  MedDRA配置: 空")
            
            # 解析WHODrug配置
            if record[6]:
                try:
                    whodrug_config = json.loads(record[6])
                    print(f"  WHODrug配置项数: {len(whodrug_config)}")
                    if whodrug_config:
                        print("  WHODrug配置样本:")
                        for j, item in enumerate(whodrug_config[:3]):
                            print(f"    {j+1}. name: {item.get('name_column')}, code: {item.get('code_column')}")
                except json.JSONDecodeError:
                    print(f"  WHODrug配置: JSON解析失败")
            else:
                print(f"  WHODrug配置: 空")
            
            print(f"  创建时间: {record[7]}")
            print(f"  更新时间: {record[8]}")
    
    # 检查其他配置表
    print("\n=== 检查其他配置表 ===")
    
    config_tables = ['meddra_configs', 'whodrug_configs']
    
    for table_name in config_tables:
        print(f"\n--- {table_name} ---")
        
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        print(f"记录数: {count}")
        
        if count > 0:
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 5")
            records = cursor.fetchall()
            
            # 获取列名
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = [col[1] for col in cursor.fetchall()]
            
            print(f"列名: {columns}")
            print("样本数据:")
            for i, record in enumerate(records):
                print(f"  {i+1}. {record}")
    
    conn.close()

if __name__ == '__main__':
    check_translation_configs()