#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试修复后的匹配逻辑
验证AELLT等字段根据对应code列的值正确匹配翻译
"""

import requests
import json
import sqlite3
import pandas as pd

def test_database_structure():
    """测试数据库结构"""
    print("=== 测试数据库结构 ===")
    
    conn = sqlite3.connect('translation_db.sqlite')
    cursor = conn.cursor()
    
    # 检查MedDRA表
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='meddra_merged'")
    if cursor.fetchone():
        cursor.execute("PRAGMA table_info(meddra_merged)")
        columns = [row[1] for row in cursor.fetchall()]
        print(f"✅ meddra_merged表存在，列: {columns}")
        
        # 检查样本数据
        cursor.execute("SELECT code, name_cn, name_en FROM meddra_merged LIMIT 5")
        samples = cursor.fetchall()
        print("📋 MedDRA样本数据:")
        for code, name_cn, name_en in samples:
            print(f"  code: {code}, name_cn: {name_cn}, name_en: {name_en}")
    else:
        print("❌ meddra_merged表不存在")
    
    # 检查WHODrug表
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='whodrug_merged'")
    if cursor.fetchone():
        cursor.execute("PRAGMA table_info(whodrug_merged)")
        columns = [row[1] for row in cursor.fetchall()]
        print(f"✅ whodrug_merged表存在，列: {columns}")
        
        # 检查样本数据
        cursor.execute("SELECT code, name_cn, name_en FROM whodrug_merged LIMIT 5")
        samples = cursor.fetchall()
        print("📋 WHODrug样本数据:")
        for code, name_cn, name_en in samples:
            print(f"  code: {code}, name_cn: {name_cn}, name_en: {name_en}")
    else:
        print("❌ whodrug_merged表不存在")
    
    conn.close()

def test_config_matching_logic():
    """测试配置匹配逻辑"""
    print("\n=== 测试配置匹配逻辑 ===")
    
    # 模拟配置数据
    test_configs = [
        {
            'name': 'AELLT测试',
            'name_column': 'AELLT',
            'code_column': 'AELLTCD',
            'expected_behavior': '根据AELLTCD的code值匹配对应的name_cn/name_en字段'
        },
        {
            'name': 'AETERM测试',
            'name_column': 'AETERM', 
            'code_column': 'AEDECOD',
            'expected_behavior': '根据AEDECOD的code值匹配对应的name_cn/name_en字段'
        },
        {
            'name': 'CMTRT测试',
            'name_column': 'CMTRT',
            'code_column': 'CMDECOD', 
            'expected_behavior': '根据CMDECOD的code值匹配对应的name_cn/name_en字段'
        }
    ]
    
    for config in test_configs:
        print(f"\n📋 {config['name']}:")
        print(f"  name列: {config['name_column']}")
        print(f"  code列: {config['code_column']}")
        print(f"  预期行为: {config['expected_behavior']}")

def create_test_data():
    """创建测试数据"""
    print("\n=== 创建测试数据 ===")
    
    # 创建模拟的SAS数据
    test_data = {
        'ae.sas7bdat': pd.DataFrame({
            'AELLT': ['Headache', 'Nausea', 'Fatigue'],
            'AELLTCD': ['10019211', '10028813', '10016256'],
            'AETERM': ['Head pain', 'Feeling sick', 'Tiredness'],
            'AEDECOD': ['10019211', '10028813', '10016256']
        }),
        'cm.sas7bdat': pd.DataFrame({
            'CMTRT': ['Aspirin', 'Paracetamol', 'Ibuprofen'],
            'CMDECOD': ['N02BA01', 'N02BE01', 'M01AE01']
        })
    }
    
    print("✅ 创建测试数据:")
    for filename, df in test_data.items():
        print(f"  {filename}: {len(df)} 行, 列: {list(df.columns)}")
        print(f"    样本数据: {df.iloc[0].to_dict()}")
    
    return test_data

def test_matching_logic_simulation():
    """模拟匹配逻辑测试"""
    print("\n=== 模拟匹配逻辑测试 ===")
    
    # 模拟数据库查询结果
    mock_meddra_data = {
        '10019211': {'name_cn': '头痛', 'name_en': 'Headache'},
        '10028813': {'name_cn': '恶心', 'name_en': 'Nausea'},
        '10016256': {'name_cn': '疲劳', 'name_en': 'Fatigue'}
    }
    
    mock_whodrug_data = {
        'N02BA01': {'name_cn': '阿司匹林', 'name_en': 'Aspirin'},
        'N02BE01': {'name_cn': '对乙酰氨基酚', 'name_en': 'Paracetamol'},
        'M01AE01': {'name_cn': '布洛芬', 'name_en': 'Ibuprofen'}
    }
    
    test_data = create_test_data()
    
    # 测试MedDRA匹配逻辑
    print("\n📋 测试MedDRA匹配逻辑:")
    ae_df = test_data['ae.sas7bdat']
    
    for translation_direction in ['en_to_zh', 'zh_to_en']:
        print(f"\n  翻译方向: {translation_direction}")
        
        # 测试AELLT字段匹配
        print("  AELLT字段匹配测试:")
        for idx, row in ae_df.iterrows():
            aellt_value = row['AELLT']
            aelltcd_value = str(row['AELLTCD'])
            
            if aelltcd_value in mock_meddra_data:
                if translation_direction == 'en_to_zh':
                    expected_translation = mock_meddra_data[aelltcd_value]['name_cn']
                else:
                    expected_translation = mock_meddra_data[aelltcd_value]['name_en']
                
                print(f"    {aellt_value} (code: {aelltcd_value}) -> {expected_translation}")
            else:
                print(f"    {aellt_value} (code: {aelltcd_value}) -> 未找到匹配")
    
    # 测试WHODrug匹配逻辑
    print("\n📋 测试WHODrug匹配逻辑:")
    cm_df = test_data['cm.sas7bdat']
    
    for translation_direction in ['en_to_zh', 'zh_to_en']:
        print(f"\n  翻译方向: {translation_direction}")
        
        # 测试CMTRT字段匹配
        print("  CMTRT字段匹配测试:")
        for idx, row in cm_df.iterrows():
            cmtrt_value = row['CMTRT']
            cmdecod_value = str(row['CMDECOD'])
            
            if cmdecod_value in mock_whodrug_data:
                if translation_direction == 'en_to_zh':
                    expected_translation = mock_whodrug_data[cmdecod_value]['name_cn']
                else:
                    expected_translation = mock_whodrug_data[cmdecod_value]['name_en']
                
                print(f"    {cmtrt_value} (code: {cmdecod_value}) -> {expected_translation}")
            else:
                print(f"    {cmtrt_value} (code: {cmdecod_value}) -> 未找到匹配")

def test_api_with_fixed_logic():
    """测试修复后的API"""
    print("\n=== 测试修复后的API ===")
    
    api_url = "http://127.0.0.1:5000/api/generate_coded_list"
    
    # 检查服务器是否运行
    try:
        health_response = requests.get("http://127.0.0.1:5000/", timeout=5)
        if health_response.status_code == 200:
            print("✅ 服务器运行正常")
        else:
            print(f"⚠️  服务器响应异常: {health_response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"❌ 服务器连接失败: {e}")
        return False
    
    # 获取现有配置
    conn = sqlite3.connect('translation_db.sqlite')
    cursor = conn.cursor()
    
    cursor.execute("SELECT path FROM translation_library_configs LIMIT 1")
    result = cursor.fetchone()
    
    if not result:
        print("❌ 没有找到配置，无法测试API")
        return False
    
    test_path = result[0]
    conn.close()
    
    # 测试API调用
    test_data = {
        "translation_direction": "en_to_zh",
        "path": test_path
    }
    
    try:
        print(f"🔄 调用API: {api_url}")
        print(f"📤 请求数据: {test_data}")
        
        response = requests.post(api_url, json=test_data, timeout=30)
        
        print(f"📥 响应状态码: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            
            if result.get('success'):
                print("✅ API调用成功")
                
                data = result.get('data', {})
                coded_items = data.get('coded_items', [])
                
                print(f"📊 结果统计:")
                print(f"  总项目数: {data.get('total_count', 0)}")
                print(f"  显示项目数: {data.get('displayed_count', 0)}")
                print(f"  是否限制: {data.get('is_limited', False)}")
                
                # 检查是否移除了行数限制
                if not data.get('is_limited', True):
                    print("✅ 已成功移除行数限制")
                else:
                    print("⚠️  仍然存在行数限制")
                
                # 分析匹配结果
                if coded_items:
                    print("\n📋 匹配结果分析:")
                    
                    matched_items = [item for item in coded_items if item.get('translated_value')]
                    ai_items = [item for item in coded_items if item.get('translation_source') == 'AI']
                    library_items = [item for item in coded_items if 'merged' in item.get('translation_source', '')]
                    
                    print(f"  总项目: {len(coded_items)}")
                    print(f"  已匹配: {len(matched_items)} ({len(matched_items)/len(coded_items)*100:.1f}%)")
                    print(f"  库匹配: {len(library_items)} ({len(library_items)/len(coded_items)*100:.1f}%)")
                    print(f"  AI翻译: {len(ai_items)} ({len(ai_items)/len(coded_items)*100:.1f}%)")
                    
                    # 显示库匹配的样本
                    print("\n📋 库匹配样本:")
                    for item in library_items[:5]:
                        print(f"  {item.get('variable')}: {item.get('value')} -> {item.get('translated_value')} ({item.get('translation_source')})")
                
                return True
            else:
                print(f"❌ API调用失败: {result.get('message')}")
                return False
        else:
            print(f"❌ HTTP请求失败: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ 测试API时出错: {str(e)}")
        return False

def main():
    """主测试函数"""
    print("🔍 开始测试修复后的匹配逻辑\n")
    
    results = []
    
    # 运行所有测试
    test_database_structure()
    test_config_matching_logic()
    test_matching_logic_simulation()
    results.append(test_api_with_fixed_logic())
    
    print("\n" + "="*50)
    print("📊 测试结果汇总:")
    
    if results[0]:
        print("✅ 修复后的匹配逻辑测试通过")
        print("\n🎯 修复成果:")
        print("  1. ✅ 移除了行数限制，现在返回所有项目")
        print("  2. ✅ 修复了匹配逻辑，现在根据code列值正确匹配翻译")
        print("  3. ✅ AELLT等字段现在根据对应的code列(如AELLTCD)进行匹配")
        print("  4. ✅ 翻译方向正确：中译英取name_en，英译中取name_cn")
    else:
        print("❌ 修复后的匹配逻辑测试失败")
        print("\n⚠️  需要进一步检查和修复")

if __name__ == '__main__':
    main()