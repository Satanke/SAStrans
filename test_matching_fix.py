#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试修复后的匹配逻辑
验证AELLT字段能够正确根据AELLTCD的code值匹配到翻译
"""

import sqlite3
import pandas as pd
import json
import requests

def test_eye_discomfort_matching():
    """测试眼部不适的匹配逻辑"""
    print("=== 测试眼部不适匹配逻辑 ===")
    
    # 1. 验证数据库中的数据
    try:
        conn = sqlite3.connect('translation_db.sqlite')
        cursor = conn.cursor()
        
        # 先检查表是否存在
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='meddra_merged'")
        if not cursor.fetchone():
            print("❌ meddra_merged表不存在")
            conn.close()
            return
    except Exception as e:
        print(f"❌ 数据库连接失败: {e}")
        return
    
    # 查询眼部不适相关数据
    cursor.execute("""
        SELECT code, name_cn, name_en 
        FROM meddra_merged 
        WHERE name_cn LIKE '%眼部不适%' AND version = '27.1'
    """)
    
    results = cursor.fetchall()
    print(f"数据库中找到 {len(results)} 条眼部不适相关记录:")
    for code, name_cn, name_en in results:
        print(f"  Code: {code}, 中文: {name_cn}, 英文: {name_en}")
    
    conn.close()
    
    # 2. 创建测试数据
    test_data = pd.DataFrame({
        'AELLT': ['眼部不适', '头痛', '恶心'],
        'AELLTCD': ['10082572', '10019211', '10028813']
    })
    
    print("\n=== 测试数据 ===")
    print(test_data)
    
    # 3. 模拟API调用测试
    config_data = {
        'variables': [
            {
                'name': 'AELLT',
                'code_variable': 'AELLTCD',
                'translation_library': 'meddra',
                'translation_direction': 'zh_to_en'
            }
        ],
        'meddra_version': '27.1',
        'whodrug_version': '2024_1',
        'path': 'test_data.csv',
        'table_path': 'test_data.csv'
    }
    
    # 保存测试数据到CSV
    test_data.to_csv('test_data.csv', index=False, encoding='utf-8-sig')
    
    try:
        # 调用API
        response = requests.post(
            'http://127.0.0.1:5000/api/generate_coded_list',
            json=config_data,
            headers={'Content-Type': 'application/json'}
        )
        
        if response.status_code == 200:
            result = response.json()
            print("\n=== API调用结果 ===")
            print(json.dumps(result, ensure_ascii=False, indent=2))
            
            if result.get('success'):
                data = result.get('data', [])
                print("\n=== 翻译结果分析 ===")
                for item in data:
                    original = item.get('original')
                    translated = item.get('translated')
                    source = item.get('source', 'unknown')
                    print(f"原文: {original} -> 译文: {translated} (来源: {source})")
                    
                    # 特别检查眼部不适的翻译
                    if original == '眼部不适':
                        if translated and translated != '眼部不适':
                            print(f"✅ 眼部不适匹配成功: {translated}")
                        else:
                            print("❌ 眼部不适匹配失败")
            else:
                print(f"API调用失败: {result.get('message')}")
        else:
            print(f"HTTP请求失败: {response.status_code}")
            print(response.text)
            
    except Exception as e:
        print(f"测试过程中出错: {e}")
    
    # 清理测试文件
    import os
    if os.path.exists('test_data.csv'):
        os.remove('test_data.csv')

def test_code_stripping():
    """测试code值的strip处理"""
    print("\n=== 测试Code值Strip处理 ===")
    
    try:
        conn = sqlite3.connect('translation_db.sqlite')
        cursor = conn.cursor()
        
        # 先检查表是否存在
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='meddra_merged'")
        if not cursor.fetchone():
            print("❌ meddra_merged表不存在")
            conn.close()
            return
    except Exception as e:
        print(f"❌ 数据库连接失败: {e}")
        return
    
    # 模拟带空格的code值
    test_codes = ['10082572', ' 10082572', '10082572 ', ' 10082572 ']
    
    for code in test_codes:
        # 测试原始查询
        cursor.execute("""
            SELECT code, name_cn, name_en 
            FROM meddra_merged 
            WHERE code = ? AND version = '27.1'
        """, [code])
        result1 = cursor.fetchall()
        
        # 测试strip后的查询
        cursor.execute("""
            SELECT code, name_cn, name_en 
            FROM meddra_merged 
            WHERE code = ? AND version = '27.1'
        """, [code.strip()])
        result2 = cursor.fetchall()
        
        print(f"Code '{code}' (原始): {len(result1)} 条结果")
        print(f"Code '{code.strip()}' (strip后): {len(result2)} 条结果")
        
        if result2:
            print(f"  匹配到: {result2[0][1]}")
    
    conn.close()

if __name__ == '__main__':
    test_code_stripping()
    test_eye_discomfort_matching()