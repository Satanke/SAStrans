#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试编码清单匹配逻辑
检查MedDRA和WHODrug的翻译匹配规则是否正确实现
"""

import sqlite3
import pandas as pd
import json

class DatabaseManager:
    """简化的数据库管理器"""
    def __init__(self):
        self.db_path = 'translation_db.sqlite'
    
    def get_translation_library_config(self, project_path):
        """获取翻译库配置"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT meddra_version, whodrug_version, ig_version, 
                       meddra_config_table, whodrug_config_table
                FROM translation_library_config 
                WHERE project_path = ?
                ORDER BY id DESC LIMIT 1
            """, (project_path,))
            
            result = cursor.fetchone()
            if not result:
                return None
            
            meddra_version, whodrug_version, ig_version, meddra_config_json, whodrug_config_json = result
            
            config = {
                'meddra_version': meddra_version,
                'whodrug_version': whodrug_version,
                'ig_version': ig_version,
                'meddra_config': json.loads(meddra_config_json) if meddra_config_json else [],
                'whodrug_config': json.loads(whodrug_config_json) if whodrug_config_json else []
            }
            
            conn.close()
            return config
            
        except Exception as e:
            print(f"获取翻译库配置失败: {e}")
            return None

def test_meddra_matching():
    """测试MedDRA匹配逻辑"""
    print("=== 测试MedDRA匹配逻辑 ===")
    
    # 连接数据库
    conn = sqlite3.connect('translation_db.sqlite')
    cursor = conn.cursor()
    
    try:
        # 检查meddra_merged表结构和数据
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='meddra_merged'")
        if not cursor.fetchone():
            print("❌ meddra_merged表不存在")
            return False
        
        # 检查表结构
        cursor.execute("PRAGMA table_info(meddra_merged)")
        columns = [row[1] for row in cursor.fetchall()]
        required_columns = ['code', 'name_cn', 'name_en', 'version']
        missing_columns = [col for col in required_columns if col not in columns]
        
        if missing_columns:
            print(f"❌ meddra_merged表缺少必要列: {missing_columns}")
            return False
        
        print(f"✅ meddra_merged表结构正确，包含列: {columns}")
        
        # 检查数据
        cursor.execute("SELECT DISTINCT version FROM meddra_merged LIMIT 10")
        versions = [row[0] for row in cursor.fetchall()]
        print(f"✅ 可用的MedDRA版本: {versions}")
        
        # 测试code匹配逻辑
        cursor.execute("SELECT code, name_cn, name_en FROM meddra_merged WHERE version = ? LIMIT 5", (versions[0] if versions else '27.1',))
        sample_data = cursor.fetchall()
        
        if sample_data:
            print("\n📋 MedDRA样本数据:")
            for code, name_cn, name_en in sample_data:
                print(f"  Code: {code}, CN: {name_cn}, EN: {name_en}")
        else:
            print("❌ meddra_merged表中没有数据")
            return False
        
        return True
        
    except Exception as e:
        print(f"❌ 测试MedDRA匹配时出错: {e}")
        return False
    finally:
        conn.close()

def test_whodrug_matching():
    """测试WHODrug匹配逻辑"""
    print("\n=== 测试WHODrug匹配逻辑 ===")
    
    # 连接数据库
    conn = sqlite3.connect('translation_db.sqlite')
    cursor = conn.cursor()
    
    try:
        # 检查whodrug_merged表结构和数据
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='whodrug_merged'")
        if not cursor.fetchone():
            print("❌ whodrug_merged表不存在")
            return False
        
        # 检查表结构
        cursor.execute("PRAGMA table_info(whodrug_merged)")
        columns = [row[1] for row in cursor.fetchall()]
        required_columns = ['code', 'name_cn', 'name_en', 'version']
        missing_columns = [col for col in required_columns if col not in columns]
        
        if missing_columns:
            print(f"❌ whodrug_merged表缺少必要列: {missing_columns}")
            return False
        
        print(f"✅ whodrug_merged表结构正确，包含列: {columns}")
        
        # 检查数据
        cursor.execute("SELECT DISTINCT version FROM whodrug_merged LIMIT 10")
        versions = [row[0] for row in cursor.fetchall()]
        print(f"✅ 可用的WHODrug版本: {versions}")
        
        # 测试样本数据
        cursor.execute("SELECT code, name_cn, name_en FROM whodrug_merged WHERE version = ? LIMIT 5", (versions[0] if versions else 'B3_202312',))
        sample_data = cursor.fetchall()
        
        if sample_data:
            print("\n📋 WHODrug样本数据:")
            for code, name_cn, name_en in sample_data:
                print(f"  Code: {code}, CN: {name_cn}, EN: {name_en}")
        else:
            print("❌ whodrug_merged表中没有数据")
            return False
        
        return True
        
    except Exception as e:
        print(f"❌ 测试WHODrug匹配时出错: {e}")
        return False
    finally:
        conn.close()

def test_translation_config():
    """测试翻译配置"""
    print("\n=== 测试翻译配置 ===")
    
    try:
        db_manager = DatabaseManager()
        
        # 获取所有项目路径的配置
        conn = sqlite3.connect('translation_db.sqlite')
        cursor = conn.cursor()
        
        cursor.execute("SELECT DISTINCT project_path FROM translation_library_config")
        paths = [row[0] for row in cursor.fetchall()]
        
        if not paths:
            print("❌ 没有找到任何翻译库配置")
            return False
        
        print(f"✅ 找到 {len(paths)} 个项目配置")
        
        # 测试第一个配置
        test_path = paths[0]
        config = db_manager.get_translation_library_config(test_path)
        
        if config:
            print(f"\n📋 测试配置 (路径: {test_path}):")
            print(f"  MedDRA版本: {config.get('meddra_version')}")
            print(f"  WHODrug版本: {config.get('whodrug_version')}")
            print(f"  MedDRA配置项数: {len(config.get('meddra_config', []))}")
            print(f"  WHODrug配置项数: {len(config.get('whodrug_config', []))}")
            
            # 检查配置项结构
            meddra_config = config.get('meddra_config', [])
            if meddra_config:
                print("\n📋 MedDRA配置项示例:")
                for i, item in enumerate(meddra_config[:3]):
                    print(f"  {i+1}. name_column: {item.get('name_column')}, code_column: {item.get('code_column')}")
            
            whodrug_config = config.get('whodrug_config', [])
            if whodrug_config:
                print("\n📋 WHODrug配置项示例:")
                for i, item in enumerate(whodrug_config[:3]):
                    print(f"  {i+1}. name_column: {item.get('name_column')}, code_column: {item.get('code_column')}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ 测试翻译配置时出错: {e}")
        return False

def test_duplicate_detection_logic():
    """测试重复值检测逻辑"""
    print("\n=== 测试重复值检测逻辑 ===")
    
    # 模拟编码清单数据
    test_coded_items = [
        {'variable': 'AEDECOD', 'value': 'Headache', 'translated_value': '头痛', 'needs_confirmation': 'N'},
        {'variable': 'AEDECOD', 'value': 'Headache', 'translated_value': '头疼', 'needs_confirmation': 'N'},  # 重复值，不同翻译
        {'variable': 'AEDECOD', 'value': 'Fever', 'translated_value': '发热', 'needs_confirmation': 'N'},
        {'variable': 'CMDECOD', 'value': 'Aspirin', 'translated_value': '阿司匹林', 'needs_confirmation': 'N'},
        {'variable': 'CMDECOD', 'value': 'Aspirin', 'translated_value': '阿司匹林', 'needs_confirmation': 'N'},  # 重复值，相同翻译
    ]
    
    # 应用重复值检测逻辑
    value_translations = {}
    for item in test_coded_items:
        key = f"{item['variable']}|{item['value']}"
        if key not in value_translations:
            value_translations[key] = set()
        if item['translated_value']:  # 只记录有翻译值的项
            value_translations[key].add(item['translated_value'])
    
    # 标记有多个翻译结果的项为待确认
    for item in test_coded_items:
        key = f"{item['variable']}|{item['value']}"
        if len(value_translations.get(key, set())) > 1:
            item['needs_confirmation'] = 'Y'
            item['highlight'] = True  # 添加高亮标记
        else:
            item['highlight'] = False
    
    print("📋 重复值检测结果:")
    for item in test_coded_items:
        highlight_status = "🔴 高亮" if item.get('highlight') else "⚪ 正常"
        confirmation_status = "需要确认" if item['needs_confirmation'] == 'Y' else "无需确认"
        print(f"  {item['variable']}|{item['value']} -> {item['translated_value']} [{highlight_status}] [{confirmation_status}]")
    
    # 验证逻辑正确性
    headache_items = [item for item in test_coded_items if item['variable'] == 'AEDECOD' and item['value'] == 'Headache']
    aspirin_items = [item for item in test_coded_items if item['variable'] == 'CMDECOD' and item['value'] == 'Aspirin']
    
    headache_should_highlight = all(item.get('highlight') for item in headache_items)
    aspirin_should_not_highlight = all(not item.get('highlight') for item in aspirin_items)
    
    if headache_should_highlight and aspirin_should_not_highlight:
        print("✅ 重复值检测逻辑正确")
        return True
    else:
        print("❌ 重复值检测逻辑有问题")
        return False

def main():
    """主测试函数"""
    print("🔍 开始测试编码清单匹配逻辑\n")
    
    results = []
    results.append(test_meddra_matching())
    results.append(test_whodrug_matching())
    results.append(test_translation_config())
    results.append(test_duplicate_detection_logic())
    
    print("\n" + "="*50)
    print("📊 测试结果汇总:")
    test_names = ["MedDRA匹配", "WHODrug匹配", "翻译配置", "重复值检测"]
    
    for i, (name, result) in enumerate(zip(test_names, results)):
        status = "✅ 通过" if result else "❌ 失败"
        print(f"  {i+1}. {name}: {status}")
    
    passed_count = sum(results)
    total_count = len(results)
    print(f"\n🎯 总体结果: {passed_count}/{total_count} 项测试通过")
    
    if passed_count == total_count:
        print("🎉 所有测试通过！")
    else:
        print("⚠️  存在问题需要修复")

if __name__ == '__main__':
    main()