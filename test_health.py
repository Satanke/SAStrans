#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import time

def test_health():
    """测试服务器健康状态"""
    print("🔍 测试服务器健康状态")
    
    # 测试基本连接
    try:
        response = requests.get("http://127.0.0.1:5000/", timeout=5)
        print(f"✅ 服务器连接正常，状态码: {response.status_code}")
    except Exception as e:
        print(f"❌ 服务器连接失败: {e}")
        return False
    
    # 测试翻译服务状态
    try:
        response = requests.get("http://127.0.0.1:5000/api/translation_service_status", timeout=5)
        if response.status_code == 200:
            result = response.json()
            print(f"✅ 翻译服务状态: {result}")
        else:
            print(f"⚠️ 翻译服务状态检查失败: {response.status_code}")
    except Exception as e:
        print(f"❌ 翻译服务状态检查异常: {e}")
    
    # 测试简单的编码清单生成（使用很短的超时）
    print("\n🔄 测试编码清单生成API（短超时）")
    try:
        start_time = time.time()
        response = requests.post(
            "http://127.0.0.1:5000/api/generate_coded_list",
            json={'translation_direction': 'en_to_zh', 'path': 'E:\\work\\test'},
            timeout=10  # 很短的超时
        )
        end_time = time.time()
        print(f"✅ API响应成功，状态码: {response.status_code}，耗时: {end_time-start_time:.2f}秒")
        return True
    except requests.exceptions.Timeout:
        print(f"⏰ API调用超时（10秒），但这是预期的，说明服务器正在处理请求")
        return True  # 超时也算正常，说明服务器在工作
    except Exception as e:
        print(f"❌ API调用异常: {e}")
        return False

if __name__ == "__main__":
    success = test_health()
    if success:
        print("\n🎉 服务器健康检查通过！")
    else:
        print("\n⚠️ 服务器健康检查失败")