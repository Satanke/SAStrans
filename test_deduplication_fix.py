#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试编码清单去重功能修复
"""

import requests
import json
import time

def test_coded_list_deduplication():
    """测试编码清单去重功能"""
    print("=== 测试编码清单去重功能 ===")
    
    # 服务器地址
    base_url = "http://localhost:5000"
    
    # 1. 测试服务器是否运行
    try:
        response = requests.get(f"{base_url}/", timeout=5)
        if response.status_code == 200:
            print("✓ 服务器运行正常")
        else:
            print(f"✗ 服务器响应异常: {response.status_code}")
            return False
    except Exception as e:
        print(f"✗ 无法连接到服务器: {e}")
        return False
    
    # 2. 测试编码清单生成API（快速测试）
    print("\n测试编码清单生成API...")
    test_data = {
        "translation_direction": "zh_to_en",
        "mode": "meddra_whodrug",
        "meddra_version": "26.1",
        "whodrug_version": "2024_1",
        "path": "E:\\work\\git\\SAStrans\\test_data"
    }
    
    try:
        response = requests.post(
            f"{base_url}/api/generate_coded_list",
            json=test_data,
            timeout=10  # 10秒超时
        )
        
        if response.status_code == 200:
            result = response.json()
            if result.get('success'):
                data = result.get('data', {})
                coded_items = data.get('coded_items', [])
                total_count = data.get('total_count', 0)
                
                print(f"✓ 编码清单生成成功")
                print(f"  - 总项目数: {total_count}")
                print(f"  - 返回项目数: {len(coded_items)}")
                
                # 检查去重效果
                unique_combinations = set()
                duplicates = []
                
                for item in coded_items:
                    key = (item.get('dataset'), item.get('variable'), item.get('value'))
                    if key in unique_combinations:
                        duplicates.append(key)
                    else:
                        unique_combinations.add(key)
                
                if duplicates:
                    print(f"✗ 发现重复项: {len(duplicates)}个")
                    for dup in duplicates[:5]:  # 只显示前5个
                        print(f"    重复: {dup}")
                    return False
                else:
                    print("✓ 去重功能正常，无重复项")
                
                # 检查翻译值为空的情况
                empty_translations = [item for item in coded_items if not item.get('translated_value')]
                if empty_translations:
                    print(f"⚠️  发现 {len(empty_translations)} 个翻译值为空的项目")
                    for item in empty_translations[:3]:  # 只显示前3个
                        print(f"    空翻译: {item.get('dataset')}.{item.get('variable')} = '{item.get('value')}'")
                else:
                    print("✓ 所有项目都有翻译值")
                
                return True
            else:
                print(f"✗ API返回失败: {result.get('message')}")
                return False
        else:
            print(f"✗ API请求失败: {response.status_code}")
            try:
                error_data = response.json()
                print(f"  错误信息: {error_data.get('message', '未知错误')}")
            except:
                print(f"  响应内容: {response.text[:200]}")
            return False
            
    except requests.exceptions.Timeout:
        print("✗ API请求超时（10秒）")
        return False
    except Exception as e:
        print(f"✗ API请求异常: {e}")
        return False

def test_frontend_debugging():
    """测试前端调试信息"""
    print("\n=== 前端调试信息测试 ===")
    print("请在浏览器中执行以下步骤：")
    print("1. 打开浏览器开发者工具（F12）")
    print("2. 切换到Console标签页")
    print("3. 访问 http://localhost:5000")
    print("4. 进入翻译与确认页面")
    print("5. 点击'编码清单'按钮")
    print("6. 查看Console中的调试信息：")
    print("   - 应该看到 'generateCodedList 函数被调用'")
    print("   - 应该看到 '获取到的配置: {...}'")
    print("   - 如果配置为空，会看到 '配置为空，显示警告'")
    print("\n如果没有看到这些信息，说明按钮点击事件没有正确绑定")

def main():
    """主测试函数"""
    print("编码清单去重功能修复测试")
    print("=" * 50)
    
    # 测试后端去重功能
    backend_success = test_coded_list_deduplication()
    
    # 提供前端调试指导
    test_frontend_debugging()
    
    print("\n=== 测试总结 ===")
    if backend_success:
        print("✓ 后端去重功能修复成功")
        print("✓ 编码清单生成API正常工作")
        print("⚠️  请按照上述步骤测试前端点击事件")
    else:
        print("✗ 后端测试失败，需要进一步检查")
    
    print("\n修复内容：")
    print("1. ✓ 添加了按数据集名称、变量名称、原始值的去重逻辑")
    print("2. ✓ 在前端generateCodedList函数中添加了调试信息")
    print("3. ⚠️  如果前端仍无反应，可能需要检查事件绑定时机")

if __name__ == "__main__":
    main()