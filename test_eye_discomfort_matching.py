#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试眼部不适匹配问题
模拟用户提到的AELLT='眼部不适', AELLTCD='10082572'的匹配情况
"""

import sqlite3
import pandas as pd

def test_eye_discomfort_matching():
    """测试眼部不适的匹配逻辑"""
    print("=== 测试眼部不适匹配逻辑 ===")
    
    # 连接数据库
    conn = sqlite3.connect('translation_db.sqlite')
    cursor = conn.cursor()
    
    try:
        # 1. 首先确认数据库中有眼部不适的记录
        cursor.execute("SELECT code, name_cn, name_en FROM meddra_merged WHERE name_cn = '眼部不适' AND version = '27.1'")
        db_result = cursor.fetchone()
        
        if db_result:
            print(f"✅ 数据库中找到眼部不适记录: Code={db_result[0]}, CN={db_result[1]}, EN={db_result[2]}")
        else:
            print("❌ 数据库中未找到眼部不适记录")
            return False
        
        # 2. 模拟用户数据
        test_data = pd.DataFrame({
            'AELLT': ['眼部不适', '头痛', '恶心'],
            'AELLTCD': ['10082572', '10019211', '10028813']
        })
        
        print("\n📋 测试数据:")
        for idx, row in test_data.iterrows():
            print(f"  {row['AELLT']} -> {row['AELLTCD']}")
        
        # 3. 模拟匹配逻辑（基于修复后的代码逻辑）
        print("\n=== 模拟匹配逻辑 ===")
        
        # 获取code列的唯一值
        code_unique_values = test_data['AELLTCD'].dropna().unique()
        print(f"Code唯一值: {list(code_unique_values)}")
        
        # 批量查询翻译数据
        placeholders = ','.join(['?'] * len(code_unique_values))
        meddra_query = f"SELECT code, name_cn, name_en FROM meddra_merged WHERE code IN ({placeholders}) AND version = ?"
        cursor.execute(meddra_query, [str(code) for code in code_unique_values] + ['27.1'])
        
        # 建立code到翻译的映射
        code_to_translation = {}
        query_results = cursor.fetchall()
        print(f"\n数据库查询结果: {len(query_results)} 条记录")
        
        for row in query_results:
            code, name_cn, name_en = row
            print(f"  Code: {code}, CN: {name_cn}, EN: {name_en}")
            # 假设翻译方向是中译英
            code_to_translation[str(code)] = name_en
        
        # 4. 为每个name值找到对应的code值，然后获取翻译
        print("\n=== 匹配结果 ===")
        
        for idx, row in test_data.iterrows():
            aellt_value = row['AELLT']
            aelltcd_value = str(row['AELLTCD'])
            
            if aelltcd_value in code_to_translation:
                translation = code_to_translation[aelltcd_value]
                print(f"✅ {aellt_value} (code: {aelltcd_value}) -> {translation}")
            else:
                print(f"❌ {aellt_value} (code: {aelltcd_value}) -> 未找到匹配")
        
        # 5. 检查可能的问题
        print("\n=== 问题排查 ===")
        
        # 检查code值的数据类型和格式
        print("Code值格式检查:")
        for code in code_unique_values:
            print(f"  原始值: '{code}', 类型: {type(code)}, 转换后: '{str(code)}'")
            
            # 检查是否有前后空格
            if isinstance(code, str):
                stripped = code.strip()
                if code != stripped:
                    print(f"    ⚠️  发现前后空格: '{code}' -> '{stripped}'")
        
        # 检查数据库中的code值格式
        cursor.execute("SELECT DISTINCT code FROM meddra_merged WHERE code IN ('10082572', ' 10082572', '10082572 ') AND version = '27.1'")
        db_codes = cursor.fetchall()
        print(f"\n数据库中相关code值: {[row[0] for row in db_codes]}")
        
        return True
        
    except Exception as e:
        print(f"❌ 测试过程中出错: {e}")
        return False
    finally:
        conn.close()

def test_string_matching_issues():
    """测试字符串匹配可能的问题"""
    print("\n=== 测试字符串匹配问题 ===")
    
    # 模拟可能的问题情况
    test_cases = [
        {'original': '10082572', 'variations': ['10082572', ' 10082572', '10082572 ', ' 10082572 ']},
        {'original': '眼部不适', 'variations': ['眼部不适', ' 眼部不适', '眼部不适 ', ' 眼部不适 ']}
    ]
    
    for case in test_cases:
        print(f"\n测试 '{case['original']}' 的变体:")
        for variation in case['variations']:
            is_equal = variation == case['original']
            stripped_equal = variation.strip() == case['original']
            print(f"  '{variation}' == '{case['original']}': {is_equal}, 去空格后: {stripped_equal}")

if __name__ == '__main__':
    test_eye_discomfort_matching()
    test_string_matching_issues()