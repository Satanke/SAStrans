#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SAS数据集翻译平台启动脚本
"""

import os
import sys
import subprocess
import webbrowser
import time
from pathlib import Path

def check_dependencies():
    """检查必要的依赖包"""
    required_packages = ['flask', 'pandas', 'pyreadstat', 'numpy']
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        print(f"❌ 缺少以下依赖包: {', '.join(missing_packages)}")
        print("请运行以下命令安装依赖:")
        print("pip install -r requirements.txt")
        return False
    
    print("✅ 所有依赖包已安装")
    return True

def start_application():
    """启动Flask应用"""
    print("🚀 正在启动SAS数据集翻译平台...")
    
    # 检查依赖
    if not check_dependencies():
        return False
    
    # 设置环境变量
    os.environ['FLASK_ENV'] = 'development'
    os.environ['FLASK_DEBUG'] = '1'
    
    try:
        # 导入并启动应用
        from app import app
        
        print("✅ 应用启动成功!")
        print("📍 访问地址: http://localhost:5000")
        print("🔄 按 Ctrl+C 停止应用")
        print("-" * 50)
        
        # 延迟后自动打开浏览器
        def open_browser():
            time.sleep(1.5)
            webbrowser.open('http://localhost:5000')
        
        # 仅在重载子进程中打开一次浏览器
        if os.environ.get('WERKZEUG_RUN_MAIN') == 'true':
            import threading
            threading.Thread(target=open_browser, daemon=True).start()
        
        # 启动Flask应用
        app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=True)
        
    except ImportError as e:
        print(f"❌ 导入错误: {e}")
        return False
    except Exception as e:
        print(f"❌ 启动失败: {e}")
        return False
    
    return True

def main():
    """主函数"""
    print("=" * 60)
    print("    SAS数据集翻译平台")
    print("    基于Flask + Bootstrap构建")
    print("=" * 60)
    
    # 检查当前目录
    current_dir = Path.cwd()
    app_file = current_dir / 'app.py'
    
    if not app_file.exists():
        print("❌ 未找到app.py文件")
        print("请确保在项目根目录下运行此脚本")
        return
    
    # 启动应用
    start_application()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n👋 应用已停止")
    except Exception as e:
        print(f"\n❌ 运行出错: {e}")
