import requests
import json

# 测试编码清单生成功能
def test_coded_list():
    url = 'http://127.0.0.1:5000/api/generate_coded_list'
    
    # 测试数据
    test_data = {
        'translation_direction': 'en_to_zh',
        'path': 'E:\\work\\test'  # 使用实际的测试数据路径
    }
    
    try:
        response = requests.post(url, json=test_data)
        print(f'状态码: {response.status_code}')
        
        if response.status_code == 200:
            result = response.json()
            print(f'成功: {result.get("success")}')
            print(f'消息: {result.get("message")}')
            
            if result.get('success'):
                data = result.get('data', {})
                coded_items = data.get('coded_items', [])
                print(f'编码清单项目数量: {len(coded_items)}')
                
                # 显示前几个项目
                for i, item in enumerate(coded_items[:5]):
                    print(f'项目 {i+1}:')
                    print(f'  数据集: {item.get("dataset")}')
                    print(f'  变量: {item.get("variable")}')
                    print(f'  值: {item.get("value")}')
                    print(f'  翻译值: {item.get("translated_value")}')
                    print(f'  翻译源: {item.get("translation_source")}')
                    print(f'  需要确认: {item.get("needs_confirmation")}')
                    print()
            else:
                print(f'错误: {result.get("message")}')
        else:
            print(f'请求失败: {response.text}')
            
    except Exception as e:
        print(f'测试失败: {str(e)}')

if __name__ == '__main__':
    test_coded_list()