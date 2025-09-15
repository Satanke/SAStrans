#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试SUPP数据合并修复验证脚本
验证generate_coded_list API是否正确合并SUPP数据
"""

import os
import pandas as pd
import numpy as np
from pathlib import Path
import tempfile
import shutil
import requests
import json

def create_test_data():
    """创建测试用的SAS数据集"""
    # 创建临时目录
    test_dir = Path(tempfile.mkdtemp(prefix='sas_test_'))
    print(f"创建测试数据目录: {test_dir}")
    
    # 创建CM数据集（用药记录）
    cm_data = pd.DataFrame({
        'STUDYID': ['STUDY01'] * 3,
        'USUBJID': ['001', '002', '003'],
        'CMSEQ': [1, 1, 1],
        'CMTRT': ['阿司匹林', '布洛芬', '对乙酰氨基酚'],
        'CMDOSE': [100, 200, 500],
        'CMDOSU': ['mg', 'mg', 'mg']
    })
    
    # 创建SUPPCM数据集（CM补充数据，包含ATC码）
    suppcm_data = pd.DataFrame({
        'STUDYID': ['STUDY01'] * 6,
        'USUBJID': ['001', '001', '002', '002', '003', '003'],
        'RDOMAIN': ['CM'] * 6,
        'IDVAR': ['CMSEQ'] * 6,
        'IDVARVAL': ['1', '1', '1', '1', '1', '1'],
        'QNAM': ['ATC1CD', 'ATC2CD', 'ATC1CD', 'ATC2CD', 'ATC1CD', 'ATC2CD'],
        'QLABEL': ['ATC Level 1 Code', 'ATC Level 2 Code'] * 3,
        'QVAL': ['N02BA', 'N02BA01', 'M01AE', 'M01AE01', 'N02BE', 'N02BE01']
    })
    
    # 保存为SAS文件（使用pandas的to_sas方法）
    try:
        cm_file = test_dir / 'CM.sas7bdat'
        suppcm_file = test_dir / 'SUPPCM.sas7bdat'
        
        # 使用pandas的to_sas方法
        cm_data.to_sas(str(cm_file), format='sas7bdat')
        suppcm_data.to_sas(str(suppcm_file), format='sas7bdat')
        print("成功创建SAS文件")
    except Exception as e:
        print(f"创建SAS文件失败: {e}，使用CSV文件代替")
        cm_file = test_dir / 'CM.csv'
        suppcm_file = test_dir / 'SUPPCM.csv'
        cm_data.to_csv(cm_file, index=False)
        suppcm_data.to_csv(suppcm_file, index=False)
        print("使用CSV文件进行测试")
    
    print(f"CM数据集: {len(cm_data)} 行")
    print(f"SUPPCM数据集: {len(suppcm_data)} 行")
    
    return test_dir, cm_file, suppcm_file

def test_api_call(test_dir):
    """测试generate_coded_list API调用"""
    url = 'http://127.0.0.1:5000/api/generate_coded_list'
   # 调用API - 需要先设置翻译库配置
    import sqlite3
    import hashlib
    
    # 创建临时翻译库配置
    conn = sqlite3.connect('translation_db.sqlite')
    cursor = conn.cursor()
    
    path_hash = hashlib.md5(str(test_dir).encode()).hexdigest()
    
    # 删除可能存在的旧配置
    cursor.execute("DELETE FROM translation_library_configs WHERE path_hash = ?", (path_hash,))
    
    # 插入测试配置
    test_meddra_config = json.dumps([{"name_column": "CMTRT", "code_column": "CMDECOD"}])
    test_whodrug_config = json.dumps([{"name_column": "CMTRT", "code_column": "CMDECOD"}])
    
    cursor.execute("""
        INSERT INTO translation_library_configs 
        (path_hash, path, mode, translation_direction, meddra_version, whodrug_version, ig_version, meddra_config, whodrug_config)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        path_hash,
        str(test_dir),
        "translation",
        "en_to_zh",
        "27.1",
        "2024 Mar 1",
        "3.4",
        test_meddra_config,
        test_whodrug_config
    ))
    
    conn.commit()
    conn.close()
    
    payload = {
        'path': str(test_dir),
        'translation_direction': 'en_to_zh'
    }
    
    try:
        print(f"调用API: {url}")
        print(f"数据路径: {test_dir}")
        
        response = requests.post(url, json=payload, timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            if result.get('success'):
                data = result.get('data', {})
                coded_items = data.get('coded_items', [])
                print(f"\n✅ API调用成功")
                print(f"返回编码项数量: {len(coded_items)}")
                
                # 检查是否包含ATC相关变量
                atc_items = [item for item in coded_items if 'ATC' in item.get('variable', '')]
                print(f"ATC相关变量数量: {len(atc_items)}")
                
                if atc_items:
                    print("\n🎉 发现ATC变量，SUPP数据合并成功！")
                    for item in atc_items[:5]:  # 显示前5个
                        print(f"  - 数据集: {item.get('dataset')}, 变量: {item.get('variable')}, 值: {item.get('value')}")
                    return True
                else:
                    print("\n❌ 未发现ATC变量，SUPP数据合并可能失败")
                    # 显示所有变量名用于调试
                    variables = set(item.get('variable', '') for item in coded_items)
                    print(f"发现的变量: {sorted(variables)}")
                    return False
            else:
                print(f"\n❌ API返回失败: {result.get('message')}")
                return False
        else:
            print(f"\n❌ HTTP错误: {response.status_code}")
            print(f"响应内容: {response.text}")
            return False
            
    except Exception as e:
        print(f"\n❌ API调用异常: {e}")
        return False

def cleanup_test_data(test_dir):
    """清理测试数据"""
    try:
        shutil.rmtree(test_dir)
        print(f"\n🧹 清理测试目录: {test_dir}")
    except Exception as e:
        print(f"\n⚠️ 清理失败: {e}")
    
    # 清理数据库中的测试配置
    try:
        import sqlite3
        import hashlib
        conn = sqlite3.connect('translation_db.sqlite')
        cursor = conn.cursor()
        path_hash = hashlib.md5(str(test_dir).encode()).hexdigest()
        cursor.execute("DELETE FROM translation_library_configs WHERE path_hash = ?", (path_hash,))
        conn.commit()
        conn.close()
        print("🧹 清理数据库测试配置")
    except Exception as e:
        print(f"清理数据库配置时出错: {e}")

def main():
    """主测试函数"""
    print("=" * 60)
    print("SUPP数据合并修复验证测试")
    print("=" * 60)
    
    # 创建测试数据
    test_dir, cm_file, suppcm_file = create_test_data()
    
    try:
        # 测试API调用
        success = test_api_call(test_dir)
        
        if success:
            print("\n✅ 测试通过：SUPP数据合并功能正常")
        else:
            print("\n❌ 测试失败：SUPP数据合并功能异常")
            
    finally:
        # 清理测试数据
        cleanup_test_data(test_dir)
    
    print("\n" + "=" * 60)
    print("测试完成")
    print("=" * 60)

if __name__ == '__main__':
    main()