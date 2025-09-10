import requests
import json

# 测试AI翻译功能
print("=== 测试AI翻译功能 ===")

# 1. 测试单个翻译
print("\n1. 测试单个翻译")
test_data = {
    'text': '手指皮肤干燥、脱屑',
    'source_lang': 'zh',
    'target_lang': 'en',
    'context': 'medical'
}

response = requests.post('http://127.0.0.1:5000/api/translate_text', json=test_data)
print(f"响应状态码: {response.status_code}")
print(f"响应内容: {response.json()}")

# 2. 测试批量翻译
print("\n2. 测试批量翻译")
batch_data = {
    'texts': ['手指皮肤干燥、脱屑', '皮肤干燥、手指皮肤脱屑', 'COVID-19感染'],
    'source_lang': 'zh',
    'target_lang': 'en',
    'context': 'medical'
}

response = requests.post('http://127.0.0.1:5000/api/batch_translate', json=batch_data)
print(f"响应状态码: {response.status_code}")
print(f"响应内容: {json.dumps(response.json(), ensure_ascii=False, indent=2)}")

# 3. 测试编码清单生成中的AI翻译逻辑
print("\n3. 测试编码清单生成")
api_data = {
    'translation_direction': 'zh_to_en',
    'path': 'E:\\work\\test'
}

response = requests.post('http://127.0.0.1:5000/api/generate_coded_list', json=api_data)
print(f"响应状态码: {response.status_code}")
result = response.json()

if result.get('success'):
    coded_items = result['data']['coded_items']
    print(f"总项目数: {len(coded_items)}")
    
    # 检查AI翻译项目
    ai_items = [item for item in coded_items if item.get('translation_source') == 'AI']
    print(f"AI翻译项目数: {len(ai_items)}")
    
    # 显示前5个AI翻译项目的详细信息
    print("\nAI翻译项目详情:")
    for i, item in enumerate(ai_items[:5]):
        print(f"  {i+1}. 原文: '{item['value']}'")
        print(f"     译文: '{item.get('translated_value', '')}'")
        print(f"     来源: {item.get('translation_source')}")
        print(f"     需确认: {item.get('needs_confirmation')}")
        print()
else:
    print(f"API调用失败: {result.get('message')}")

print("调试完成")