import requests
import json

def test_translation_with_version():
    """测试带版本参数的翻译功能"""
    print("=== 测试带版本参数的翻译功能 ===")
    
    # 测试数据
    test_data = {
        'data': [
            {'AEDECOD': '10000001'},  # MedDRA code
            {'CMDECOD': '000001010016N'}  # WHODrug code
        ],
        'meddra_version': '14.1',
        'whodrug_version': '2017 Sep 1',
        'translation_type': 'cn_to_en'
    }
    
    try:
        # 发送翻译请求
        response = requests.post(
            'http://127.0.0.1:5000/api/translate_text',
            json=test_data,
            headers={'Content-Type': 'application/json'}
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"翻译请求成功")
            print(f"返回数据: {json.dumps(result, indent=2, ensure_ascii=False)}")
            
            # 检查是否有翻译结果
            if 'data' in result and result['data']:
                translated_data = result['data']
                print(f"\n翻译结果数量: {len(translated_data)}")
                
                for i, item in enumerate(translated_data):
                    print(f"记录 {i+1}: {item}")
            else:
                print("没有返回翻译数据")
        else:
            print(f"翻译请求失败: {response.status_code}")
            print(f"错误信息: {response.text}")
            
    except Exception as e:
        print(f"翻译测试错误: {e}")

def test_version_specific_query():
    """测试版本特定的查询"""
    print("\n=== 测试版本特定查询 ===")
    
    # 测试获取特定版本的数据
    test_queries = [
        {
            'url': 'http://127.0.0.1:5000/api/get_meddra_data',
            'params': {'version': '14.1', 'limit': 5},
            'name': 'MedDRA 14.1'
        },
        {
            'url': 'http://127.0.0.1:5000/api/get_whodrug_data', 
            'params': {'version': '2017 Sep 1', 'limit': 5},
            'name': 'WHODrug 2017 Sep 1'
        }
    ]
    
    for query in test_queries:
        try:
            response = requests.get(query['url'], params=query['params'])
            if response.status_code == 200:
                data = response.json()
                print(f"{query['name']} 数据获取成功，记录数: {len(data.get('data', []))}")
                if data.get('data'):
                    print(f"示例数据: {data['data'][0]}")
            else:
                print(f"{query['name']} 数据获取失败: {response.status_code}")
        except Exception as e:
            print(f"{query['name']} 查询错误: {e}")

if __name__ == '__main__':
    test_translation_with_version()
    test_version_specific_query()