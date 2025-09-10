#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import json
import time

def test_api_simple():
    """简化的API测试"""
    print("🔍 开始简化API测试")
    
    api_url = "http://127.0.0.1:5000/api/generate_coded_list"
    test_data = {
        'translation_direction': 'en_to_zh',
        'path': 'E:\\work\\test'
    }
    
    print(f"🔄 调用API: {api_url}")
    print(f"📤 请求数据: {test_data}")
    
    try:
        # 使用较短的超时时间
        start_time = time.time()
        response = requests.post(api_url, json=test_data, timeout=60)
        end_time = time.time()
        
        print(f"📥 响应状态码: {response.status_code}")
        print(f"⏱️ 处理时间: {end_time - start_time:.2f}秒")
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ API调用成功")
            print(f"📊 结果统计:")
            if 'summary' in result:
                summary = result['summary']
                print(f"  - 总项目数: {summary.get('total_items', 'N/A')}")
                print(f"  - 已翻译项目: {summary.get('translated_items', 'N/A')}")
                print(f"  - AI翻译项目: {summary.get('ai_translated_items', 'N/A')}")
                print(f"  - 需确认项目: {summary.get('needs_confirmation', 'N/A')}")
            return True
        else:
            print(f"❌ API调用失败: {response.status_code}")
            print(f"错误信息: {response.text}")
            return False
            
    except requests.exceptions.Timeout:
        print(f"⏰ 请求超时（60秒）")
        return False
    except Exception as e:
        print(f"❌ 请求异常: {e}")
        return False

if __name__ == "__main__":
    success = test_api_simple()
    if success:
        print("\n🎉 API测试成功！")
    else:
        print("\n⚠️ API测试失败")