#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试实际的编码清单生成
模拟真实的API调用来检查匹配逻辑
"""

import requests
import json
import sqlite3

def test_coded_list_generation():
    """测试编码清单生成API"""
    print("=== 测试编码清单生成API ===")
    
    # 首先检查是否有配置数据
    conn = sqlite3.connect('translation_db.sqlite')
    cursor = conn.cursor()
    
    # 检查translation_library_configs表（注意是复数）
    cursor.execute("SELECT path FROM translation_library_configs LIMIT 1")
    result = cursor.fetchone()
    
    if not result:
        print("❌ 没有找到任何翻译库配置，创建测试配置")
        
        # 创建一个测试配置
        test_path = "E:\\test\\project"
        test_meddra_config = [
            {"name_column": "AETERM", "code_column": "AEDECOD"},
            {"name_column": "AEBODSYS", "code_column": ""}
        ]
        test_whodrug_config = [
            {"name_column": "CMTRT", "code_column": "CMDECOD"},
            {"name_column": "CONMED", "code_column": ""}
        ]
        
        import hashlib
        path_hash = hashlib.md5(test_path.encode()).hexdigest()
        
        cursor.execute("""
            INSERT INTO translation_library_configs 
            (path_hash, path, mode, translation_direction, meddra_version, whodrug_version, ig_version, meddra_config, whodrug_config)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            path_hash,
            test_path,
            "translation",
            "en_to_zh",
            "27.1",
            "2024 Mar 1", 
            "3.4",
            json.dumps(test_meddra_config),
            json.dumps(test_whodrug_config)
        ))
        conn.commit()
        print(f"✅ 创建测试配置: {test_path}")
    else:
        test_path = result[0]
        print(f"✅ 使用现有配置路径: {test_path}")
    
    # 获取配置详情
    cursor.execute("""
        SELECT meddra_version, whodrug_version, meddra_config, whodrug_config
        FROM translation_library_configs 
        WHERE path = ?
        ORDER BY id DESC LIMIT 1
    """, (test_path,))
    
    config_result = cursor.fetchone()
    if not config_result:
        print("❌ 无法获取配置详情")
        return False
    
    meddra_version, whodrug_version, meddra_config_json, whodrug_config_json = config_result
    
    print(f"📋 配置信息:")
    print(f"  MedDRA版本: {meddra_version}")
    print(f"  WHODrug版本: {whodrug_version}")
    
    # 解析配置
    try:
        meddra_config = json.loads(meddra_config_json) if meddra_config_json else []
        whodrug_config = json.loads(whodrug_config_json) if whodrug_config_json else []
        
        print(f"  MedDRA配置项: {len(meddra_config)}")
        print(f"  WHODrug配置项: {len(whodrug_config)}")
        
        # 显示配置项详情
        if meddra_config:
            print("\n📋 MedDRA配置项:")
            for i, item in enumerate(meddra_config[:5]):
                print(f"  {i+1}. name_column: {item.get('name_column')}, code_column: {item.get('code_column')}")
        
        if whodrug_config:
            print("\n📋 WHODrug配置项:")
            for i, item in enumerate(whodrug_config[:5]):
                print(f"  {i+1}. name_column: {item.get('name_column')}, code_column: {item.get('code_column')}")
        
    except json.JSONDecodeError as e:
        print(f"❌ 配置JSON解析失败: {e}")
        return False
    
    conn.close()
    
    # 测试API调用
    print("\n=== 测试API调用 ===")
    
    api_url = "http://127.0.0.1:5000/api/generate_coded_list"
    
    test_data = {
        "translation_direction": "en_to_zh",
        "path": test_path
    }
    
    try:
        print(f"🔄 调用API: {api_url}")
        print(f"📤 请求数据: {test_data}")
        
        response = requests.post(api_url, json=test_data, timeout=120)
        
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
                
                # 分析翻译结果
                if coded_items:
                    print("\n📋 翻译结果分析:")
                    
                    total_items = len(coded_items)
                    translated_items = len([item for item in coded_items if item.get('translated_value')])
                    ai_translated_items = len([item for item in coded_items if item.get('translation_source') == 'AI'])
                    needs_confirmation_items = len([item for item in coded_items if item.get('needs_confirmation') == 'Y'])
                    highlighted_items = len([item for item in coded_items if item.get('highlight')])
                    
                    print(f"  总项目: {total_items}")
                    print(f"  已翻译: {translated_items} ({translated_items/total_items*100:.1f}%)")
                    print(f"  AI翻译: {ai_translated_items} ({ai_translated_items/total_items*100:.1f}%)")
                    print(f"  需确认: {needs_confirmation_items} ({needs_confirmation_items/total_items*100:.1f}%)")
                    print(f"  高亮项: {highlighted_items} ({highlighted_items/total_items*100:.1f}%)")
                    
                    # 显示前几个样本
                    print("\n📋 样本数据:")
                    for i, item in enumerate(coded_items[:10]):
                        highlight_mark = "🔴" if item.get('highlight') else "⚪"
                        confirmation_mark = "❓" if item.get('needs_confirmation') == 'Y' else "✅"
                        translation_source = item.get('translation_source', 'unknown')
                        
                        print(f"  {i+1}. {highlight_mark}{confirmation_mark} {item.get('dataset')}.{item.get('variable')} = '{item.get('value')}' -> '{item.get('translated_value')}' [{translation_source}]")
                    
                    # 检查问题
                    problems = []
                    
                    if translated_items == 0:
                        problems.append("❌ 所有翻译值都为空")
                    
                    if ai_translated_items == total_items:
                        problems.append("❌ 所有项目都由AI翻译（翻译库匹配失败）")
                    
                    if highlighted_items == 0 and needs_confirmation_items > 0:
                        problems.append("⚠️  有需要确认的项目但没有高亮标记")
                    
                    if problems:
                        print("\n🚨 发现的问题:")
                        for problem in problems:
                            print(f"  {problem}")
                        return False
                    else:
                        print("\n🎉 编码清单生成逻辑正常")
                        return True
                else:
                    print("❌ 没有返回编码项目")
                    return False
            else:
                print(f"❌ API返回失败: {result.get('message')}")
                return False
        else:
            print(f"❌ API调用失败，状态码: {response.status_code}")
            print(f"响应内容: {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ 网络请求失败: {e}")
        return False
    except Exception as e:
        print(f"❌ 测试过程中出错: {e}")
        return False

def main():
    """主函数"""
    print("🔍 开始测试实际编码清单生成\n")
    
    success = test_coded_list_generation()
    
    print("\n" + "="*50)
    if success:
        print("🎉 编码清单测试通过！")
    else:
        print("⚠️  编码清单测试失败，需要修复")

if __name__ == '__main__':
    main()