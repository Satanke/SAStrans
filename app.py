from flask import Flask, render_template, request, jsonify, send_file
import pandas as pd
import pyreadstat
import os
import glob
import json
import numpy as np
import sqlite3
from pathlib import Path
from datetime import datetime
import hashlib
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from data_translation import data_translation_bp

# 加载环境变量
try:
    from dotenv import load_dotenv
    load_dotenv()  # 加载.env文件中的环境变量
except ImportError:
    print("Warning: python-dotenv not installed. Environment variables from .env file will not be loaded.")
    print("Install with: pip install python-dotenv")

# 禁用Pandas未来版本的降级行为警告
try:
    pd.set_option('future.no_silent_downcasting', True)
except Exception:
    pass  # 如果选项不存在则忽略

app = Flask(__name__)

# 注册蓝图
app.register_blueprint(data_translation_bp)



class DatabaseManager:
    def __init__(self, db_path='translation_db.sqlite'):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """初始化数据库表结构"""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            
            # 映射配置表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS mapping_configs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    path_hash TEXT NOT NULL,
                    path TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    translation_direction TEXT NOT NULL,
                    name TEXT NOT NULL,
                    configs TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # MedDRA数据库表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS meddra_tables (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE NOT NULL,
                    description TEXT,
                    file_path TEXT,
                    record_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # WhoDrug数据库表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS whodrug_tables (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE NOT NULL,
                    description TEXT,
                    file_path TEXT,
                    record_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # 翻译库表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS translation_library (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_text TEXT NOT NULL,
                    target_text TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    confidence REAL DEFAULT 1.0,
                    verified BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # 翻译库配置表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS translation_library_configs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    path_hash TEXT NOT NULL,
                    path TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    translation_direction TEXT NOT NULL,
                    meddra_version TEXT,
                    whodrug_version TEXT,
                    ig_version TEXT,
                    meddra_config TEXT,
                    whodrug_config TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # 合并配置表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS merge_configs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    path_hash TEXT NOT NULL,
                    path TEXT NOT NULL,
                    translation_direction TEXT NOT NULL,
                    configs TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # 翻译结果表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS translation_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    path_hash TEXT NOT NULL,         -- 路径哈希值，用于关联特定的SAS文件路径
                    translation_direction TEXT NOT NULL, -- 翻译方向（中译英/英译中）
                    translation_type TEXT NOT NULL,  -- 翻译类型（编码清单/非编码清单/数据集label/变量label）
                    dataset_name TEXT NOT NULL,      -- 数据集名称
                    variable_name TEXT NOT NULL,     -- 变量名称
                    original_value TEXT NOT NULL,    -- 原始值
                    translated_value TEXT NOT NULL,  -- 翻译值
                    translation_source TEXT NOT NULL, -- 翻译来源（database/AI）
                    needs_confirmation BOOLEAN DEFAULT FALSE, -- 是否需要确认（布尔值）
                    is_confirmed BOOLEAN DEFAULT FALSE,       -- 是否已确认（布尔值）
                    confidence_score REAL DEFAULT 1.0,       -- 置信度分数（0.0-1.0）
                    comments TEXT DEFAULT '',                 -- 备注信息
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- 创建时间
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- 更新时间
                    UNIQUE(path_hash, dataset_name, variable_name, original_value, translation_type, translation_direction)
                )
            ''')
            
            conn.commit()
        finally:
            conn.close()
    
    def save_mapping_config(self, path, mode, translation_direction, configs, name):
        """保存映射配置"""
        path_hash = hashlib.md5(path.encode()).hexdigest()
        configs_json = json.dumps(configs, ensure_ascii=False)
        
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO translation_configs 
                (path_hash, path, mode, translation_direction, name, configs, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (path_hash, path, mode, translation_direction, name, configs_json, datetime.now().isoformat()))
            conn.commit()
            return True
        except Exception as e:
            print(f"Error saving mapping config: {e}")
            return False
        finally:
            conn.close()
    
    def get_mapping_config(self, path):
        """根据路径获取映射配置"""
        path_hash = hashlib.md5(path.encode()).hexdigest()
        
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT configs, translation_direction, name FROM mapping_configs 
                WHERE path_hash = ? ORDER BY updated_at DESC LIMIT 1
            ''', (path_hash,))
            result = cursor.fetchone()
            if result:
                return {
                    'configs': json.loads(result[0]),
                    'translation_direction': result[1],
                    'name': result[2]
                }
            return None
        finally:
            conn.close()
    
    def save_translation_library_config(self, path, mode, translation_direction, meddra_version, whodrug_version, ig_version, meddra_config, whodrug_config):
        """保存翻译库配置"""
        path_hash = hashlib.md5(path.encode()).hexdigest()
        meddra_config_json = json.dumps(meddra_config, ensure_ascii=False) if meddra_config else None
        whodrug_config_json = json.dumps(whodrug_config, ensure_ascii=False) if whodrug_config else None
        
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO translation_library_configs 
                (path_hash, path, mode, translation_direction, meddra_version, whodrug_version, ig_version, meddra_config, whodrug_config, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (path_hash, path, mode, translation_direction, meddra_version, whodrug_version, ig_version, meddra_config_json, whodrug_config_json, datetime.now().isoformat()))
            conn.commit()
            return True
        except Exception as e:
            print(f"Error saving translation library config: {e}")
            return False
        finally:
            conn.close()
    
    def get_translation_library_config(self, path):
        """根据路径获取翻译库配置"""
        path_hash = hashlib.md5(path.encode()).hexdigest()
        
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT translation_direction, meddra_version, whodrug_version, ig_version, meddra_config, whodrug_config FROM translation_library_configs 
                WHERE path_hash = ? ORDER BY updated_at DESC LIMIT 1
            ''', (path_hash,))
            result = cursor.fetchone()
            if result:
                return {
                    'translation_direction': result[0],
                    'meddra_version': result[1],
                    'whodrug_version': result[2],
                    'ig_version': result[3],
                    'meddra_config': json.loads(result[4]) if result[4] else None,
                    'whodrug_config': json.loads(result[5]) if result[5] else None
                }
            return None
        finally:
            conn.close()
    
    def save_merge_config(self, path, translation_direction, configs):
        """保存合并配置"""
        path_hash = hashlib.md5(path.encode()).hexdigest()
        configs_json = json.dumps(configs, ensure_ascii=False)
        
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO merge_configs 
                (path_hash, path, translation_direction, configs, updated_at)
                VALUES (?, ?, ?, ?, ?)
            ''', (path_hash, path, translation_direction, configs_json, datetime.now().isoformat()))
            conn.commit()
            return True
        except Exception as e:
            print(f"Error saving merge config: {e}")
            return False
        finally:
            conn.close()
    
    def get_merge_config(self, path):
        """根据路径获取合并配置"""
        path_hash = hashlib.md5(path.encode()).hexdigest()
        
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT configs, translation_direction FROM merge_configs 
                WHERE path_hash = ? ORDER BY updated_at DESC LIMIT 1
            ''', (path_hash,))
            result = cursor.fetchone()
            if result:
                return {
                    'configs': json.loads(result[0]),
                    'translation_direction': result[1]
                }
            return None
        finally:
            conn.close()
    
    def get_database_tables(self):
        """获取所有数据库表信息"""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            
            # 获取MedDRA表
            cursor.execute('SELECT name, description, record_count FROM meddra_tables ORDER BY name')
            meddra_tables = [{'name': row[0], 'description': row[1], 'record_count': row[2]} 
                           for row in cursor.fetchall()]
            
            # 获取WhoDrug表
            cursor.execute('SELECT name, description, record_count FROM whodrug_tables ORDER BY name')
            whodrug_tables = [{'name': row[0], 'description': row[1], 'record_count': row[2]} 
                            for row in cursor.fetchall()]
            
            return {
                'meddra': meddra_tables,
                'whodrug': whodrug_tables
            }
        finally:
            conn.close()
    
    def create_table(self, category, name, description=''):
        """创建新的数据表"""
        table_name = f"{category}_tables"
        
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute(f'''
                INSERT INTO {table_name} (name, description)
                VALUES (?, ?)
            ''', (name, description))
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False  # 表名已存在
        except Exception as e:
            print(f"Error creating table: {e}")
            return False
        finally:
            conn.close()
    
    def delete_table(self, category, table_name):
        """删除数据表"""
        table = f"{category}_tables"
        
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute(f'DELETE FROM {table} WHERE name = ?', (table_name,))
            conn.commit()
            return True
        except Exception as e:
            print(f"Error deleting table: {e}")
            return False
        finally:
            conn.close()
    
    def check_existing_library(self):
        """检查是否存在已有的翻译库"""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM translation_library WHERE verified = TRUE')
            count = cursor.fetchone()[0]
            return count > 0
        finally:
            conn.close()
    
    def get_variable_translation(self, dataset_name, variable_name, translation_direction, ig_version):
        """从variablelabel_mergeds表获取变量标签翻译"""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT name_cn, name_en FROM variablelabel_mergeds 
                WHERE domain = ? AND variable = ? AND version = ?
            ''', (dataset_name, variable_name, ig_version))
            result = cursor.fetchone()
            if result:
                return {
                    'name_cn': result[0],
                    'name_en': result[1]
                }
            return None
        finally:
            conn.close()
    
    def get_batch_variable_translations(self, variable_list, ig_version):
        """批量获取变量标签翻译 - 使用pandas优化查询效率"""
        try:
            if not variable_list:
                return {}
            
            conn = sqlite3.connect(self.db_path)
            
            # 创建输入数据的DataFrame
            input_df = pd.DataFrame(variable_list, columns=['domain', 'variable'])
            
            # 从数据库读取所有匹配版本的翻译数据
            query = '''
                SELECT domain, variable, name_cn, name_en 
                FROM variablelabel_mergeds
                WHERE version = ?
            '''
            
            # 使用pandas直接从数据库读取数据
            translation_df = pd.read_sql_query(query, conn, params=[ig_version])
            
            # 为大小写不敏感的连接创建临时列
            input_df['domain_lower'] = input_df['domain'].str.lower()
            input_df['variable_lower'] = input_df['variable'].str.lower()
            translation_df['domain_lower'] = translation_df['domain'].str.lower()
            translation_df['variable_lower'] = translation_df['variable'].str.lower()
            
            # 使用pandas的left join进行高效连接（大小写不敏感）
            result_df = input_df.merge(
                translation_df, 
                on=['domain_lower', 'variable_lower'], 
                how='left',
                suffixes=('', '_db')
            )
            
            # 构建结果字典（使用原始的domain和variable作为key）
            translation_dict = {}
            for _, row in result_df.iterrows():
                key = (row['domain'], row['variable'])  # 使用输入的原始值作为key
                if pd.notna(row.get('name_cn')) or pd.notna(row.get('name_en')):
                    translation_dict[key] = {
                        'name_cn': row.get('name_cn'),
                        'name_en': row.get('name_en')
                    }
            
            return translation_dict
            
        except Exception as e:
            print(f"批量获取变量翻译时出错: {e}")
            return {}
        finally:
            if conn:
                conn.close()
    
    def get_batch_dataset_translations(self, dataset_list, ig_version):
        """批量获取数据集标签翻译 - 参考get_batch_variable_translations的实现"""
        try:
            if not dataset_list:
                return {}
            
            conn = sqlite3.connect(self.db_path)
            
            # 创建输入数据的DataFrame
            input_df = pd.DataFrame(dataset_list, columns=['dataset'])
            
            # 从数据库读取所有匹配版本的翻译数据
            query = '''
                SELECT dataset, name_cn, name_en 
                FROM datalabel_mergeds
                WHERE version = ?
            '''
            
            # 使用pandas直接从数据库读取数据
            translation_df = pd.read_sql_query(query, conn, params=[ig_version])
            
            # 为大小写不敏感的连接创建临时列
            input_df['dataset_lower'] = input_df['dataset'].str.lower()
            translation_df['dataset_lower'] = translation_df['dataset'].str.lower()
            
            # 使用pandas的left join进行高效连接（大小写不敏感）
            result_df = input_df.merge(
                translation_df, 
                on='dataset_lower', 
                how='left',
                suffixes=('', '_db')
            )
            
            # 构建结果字典（使用原始的dataset作为key）
            translation_dict = {}
            for _, row in result_df.iterrows():
                key = row['dataset']  # 使用输入的原始值作为key
                if pd.notna(row.get('name_cn')) or pd.notna(row.get('name_en')):
                    translation_dict[key] = {
                        'name_cn': row.get('name_cn'),
                        'name_en': row.get('name_en')
                    }
            
            return translation_dict
            
        except Exception as e:
            print(f"批量获取数据集翻译时出错: {e}")
            return {}
        finally:
            if conn:
                conn.close()
    
    def save_translation_result(self, path, translation_direction, translation_type, dataset_name, variable_name,
                              original_value, translated_value, translation_source, 
                              needs_confirmation=False, confidence_score=1.0, comments=''):
        """保存翻译结果"""
        path_hash = hashlib.md5(path.encode()).hexdigest()
        
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO translation_results 
                (path_hash, translation_direction, translation_type, dataset_name, variable_name, original_value, 
                 translated_value, translation_source, needs_confirmation, confidence_score, comments, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (path_hash, translation_direction, translation_type, dataset_name, variable_name, original_value, 
                  translated_value, translation_source, needs_confirmation, confidence_score, comments, datetime.now().isoformat()))
            conn.commit()
            return True
        except Exception as e:
            print(f"Error saving translation result: {e}")
            return False
        finally:
            conn.close()
    
    def get_sdtm_dataset_translation(self, dataset_name, translation_direction, ig_version):
        """从datalabel_mergeds表查找数据集翻译"""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            
            # 查询datalabel_mergeds表
            cursor.execute('''
                SELECT dataset, name_cn, name_en, version 
                FROM datalabel_mergeds 
                WHERE UPPER(dataset) = UPPER(?) AND version = ?
                LIMIT 1
            ''', (dataset_name, ig_version))
            
            result = cursor.fetchone()
            if result:
                return {
                    'dataset': result[0],
                    'name_cn': result[1],
                    'name_en': result[2],
                    'version': result[3]
                }
            
            # 如果指定版本没找到，尝试查找任意版本
            cursor.execute('''
                SELECT dataset, name_cn, name_en, version 
                FROM datalabel_mergeds 
                WHERE UPPER(dataset) = UPPER(?)
                ORDER BY version DESC
                LIMIT 1
            ''', (dataset_name,))
            
            result = cursor.fetchone()
            if result:
                return {
                    'dataset': result[0],
                    'name_cn': result[1],
                    'name_en': result[2],
                    'version': result[3]
                }
            
            return None
        except Exception as e:
            print(f"Error querying SDTM dataset translation: {e}")
            return None
        finally:
            conn.close()
    
    def create_datalabel_mergeds_table(self):
        """创建并合并指定的SDTM表为datalabel_mergeds表"""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            
            # 创建合并表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS datalabel_mergeds (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    dataset TEXT NOT NULL,
                    name_cn TEXT,
                    name_en TEXT,
                    version TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(dataset, version)
                )
            ''')
            
            # 指定要合并的6张表
            target_tables = [
                {'name': 'sdtm2dataset3_2cn', 'version': '3.2', 'lang': 'cn'},
                {'name': 'sdtm2dataset3_2en', 'version': '3.2', 'lang': 'en'},
                {'name': 'sdtm2dataset3_3cn', 'version': '3.3', 'lang': 'cn'},
                {'name': 'sdtm2dataset3_3en', 'version': '3.3', 'lang': 'en'},
                {'name': 'sdtm2dataset3_4cn', 'version': '3.4', 'lang': 'cn'},
                {'name': 'sdtm2dataset3_4en', 'version': '3.4', 'lang': 'en'}
            ]
            
            # 清空现有数据
            cursor.execute('DELETE FROM datalabel_mergeds')
            
            # 存储每个数据集的信息
            dataset_info = {}
            
            # 处理每张表
            for table_config in target_tables:
                table_name = table_config['name']
                version = table_config['version']
                lang = table_config['lang']
                
                try:
                    # 检查表是否存在
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
                    if not cursor.fetchone():
                        print(f"表 {table_name} 不存在，跳过")
                        continue
                    
                    # 检查表结构
                    cursor.execute(f"PRAGMA table_info({table_name})")
                    columns = [row[1] for row in cursor.fetchall()]
                    
                    # 查找数据集和描述列
                    dataset_col = None
                    desc_col = None
                    
                    print(f"表 {table_name} 的列: {columns}")
                    
                    for col in columns:
                        col_upper = col.upper()
                        if col_upper in ['DATASET', 'DOMAIN']:
                            dataset_col = col
                        elif col_upper in ['DESCRIPTION', 'DESC', 'NAME', 'LABEL']:
                            desc_col = col
                    
                    # 如果没找到标准列名，尝试直接匹配
                    if not dataset_col:
                        for col in columns:
                            if 'DATASET' in col.upper():
                                dataset_col = col
                                break
                    
                    if not desc_col:
                        for col in columns:
                            if any(keyword in col.upper() for keyword in ['DESCRIPTION', 'DESC', 'NAME', 'LABEL']):
                                desc_col = col
                                break
                    
                    print(f"找到的列: dataset_col={dataset_col}, desc_col={desc_col}")
                    
                    if not dataset_col or not desc_col:
                        print(f"表 {table_name} 缺少必要的列，跳过")
                        continue
                    
                    # 读取数据
                    cursor.execute(f"SELECT {dataset_col}, {desc_col} FROM {table_name} WHERE {dataset_col} IS NOT NULL AND {dataset_col} != ''")
                    rows = cursor.fetchall()
                    
                    # 处理数据
                    for row in rows:
                        dataset = row[0].strip()
                        description = row[1].strip() if row[1] else ''
                        
                        # 初始化数据集信息
                        if dataset not in dataset_info:
                            dataset_info[dataset] = {}
                        if version not in dataset_info[dataset]:
                            dataset_info[dataset][version] = {'name_cn': '', 'name_en': ''}
                        
                        # 根据语言设置对应字段
                        if lang == 'cn':
                            dataset_info[dataset][version]['name_cn'] = description
                        else:
                            dataset_info[dataset][version]['name_en'] = description
                    
                    print(f"已处理表 {table_name} (版本 {version}, 语言 {lang})")
                    
                except sqlite3.OperationalError as e:
                    print(f"处理表 {table_name} 时出错: {e}")
                    continue
            
            # 将合并后的数据插入到datalabel_mergeds表
            insert_count = 0
            for dataset, versions in dataset_info.items():
                for version, names in versions.items():
                    cursor.execute('''
                        INSERT OR REPLACE INTO datalabel_mergeds (dataset, name_cn, name_en, version)
                        VALUES (?, ?, ?, ?)
                    ''', (dataset, names['name_cn'], names['name_en'], version))
                    insert_count += 1
            
            conn.commit()
            
            print(f"datalabel_mergeds表创建完成，共 {insert_count} 条记录")
            
            return True
            
        except Exception as e:
            print(f"创建datalabel_mergeds表失败: {e}")
            return False
        finally:
            conn.close()
    
    def get_translation_results(self, path, translation_type=None, dataset_name=None, 
                              variable_name=None, page=1, page_size=50):
        """获取翻译结果"""
        path_hash = hashlib.md5(path.encode()).hexdigest()
        
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            
            # 构建查询条件
            query = '''
                SELECT id, translation_type, dataset_name, variable_name, original_value, 
                       translated_value, translation_source, needs_confirmation, is_confirmed, 
                       confidence_score, comments, created_at, updated_at
                FROM translation_results 
                WHERE path_hash = ?
            '''
            params = [path_hash]
            
            if translation_type:
                query += ' AND translation_type = ?'
                params.append(translation_type)
            
            if dataset_name:
                query += ' AND dataset_name = ?'
                params.append(dataset_name)
                
            if variable_name:
                query += ' AND variable_name = ?'
                params.append(variable_name)
            
            # 获取总数
            count_query = query.replace(
                'SELECT id, translation_type, dataset_name, variable_name, original_value, translated_value, translation_source, needs_confirmation, is_confirmed, confidence_score, comments, created_at, updated_at',
                'SELECT COUNT(*)'
            )
            cursor.execute(count_query, params)
            total_count = cursor.fetchone()[0]
            
            # 添加排序和分页
            query += ' ORDER BY translated_value DESC, translation_source DESC, needs_confirmation DESC, is_confirmed DESC, translation_type ASC, dataset_name ASC, variable_name ASC'
            if page_size > 0:
                offset = (page - 1) * page_size
                query += f' LIMIT {page_size} OFFSET {offset}'
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            data = [{
                'id': row[0],
                'translation_type': row[1],
                'dataset_name': row[2],
                'variable_name': row[3],
                'original_value': row[4],
                'translated_value': row[5],
                'translation_source': row[6],
                'needs_confirmation': bool(row[7]),
                'is_confirmed': bool(row[8]),
                'confidence_score': row[9],
                'comments': row[10],
                'created_at': row[11],
                'updated_at': row[12]
            } for row in results]
            
            return {
                'items': data,
                'total': total_count,
                'page': page,
                'page_size': page_size,
                'total_pages': (total_count + page_size - 1) // page_size if page_size > 0 else 1
            }
        finally:
            conn.close()
    
    def update_translation_confirmation(self, path, translation_type, dataset_name, 
                                      variable_name, original_value, is_confirmed):
        """更新翻译确认状态"""
        path_hash = hashlib.md5(path.encode()).hexdigest()
        
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE translation_results 
                SET is_confirmed = ?, updated_at = ?
                WHERE path_hash = ? AND translation_type = ? AND dataset_name = ? 
                      AND variable_name = ? AND original_value = ?
            ''', (is_confirmed, datetime.now().isoformat(), path_hash, translation_type, 
                  dataset_name, variable_name, original_value))
            conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            print(f"Error updating translation confirmation: {e}")
            return False
        finally:
            conn.close()
    
    def get_translation_result_by_id(self, result_id):
        """根据ID获取翻译结果记录"""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id, translation_type, dataset_name, variable_name, original_value, 
                       translated_value, translation_source, needs_confirmation, is_confirmed, 
                       confidence_score, created_at, updated_at
                FROM translation_results 
                WHERE id = ?
            ''', (result_id,))
            
            row = cursor.fetchone()
            if row:
                return {
                    'id': row[0],
                    'translation_type': row[1],
                    'dataset_name': row[2],
                    'variable_name': row[3],
                    'original_value': row[4],
                    'translated_value': row[5],
                    'translation_source': row[6],
                    'needs_confirmation': bool(row[7]),
                    'is_confirmed': bool(row[8]),
                    'confidence_score': row[9],
                    'created_at': row[10],
                    'updated_at': row[11]
                }
            return None
        except Exception as e:
            print(f"Error getting translation result by ID: {e}")
            return None
        finally:
            conn.close()
    
    def update_translation_result(self, result_id, translated_value=None,
                                needs_confirmation=None, is_confirmed=None, translation_source=None, comments=None):
        """更新翻译结果"""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            
            # 构建更新语句
            update_fields = []
            params = []
            
            if translated_value is not None:
                update_fields.append('translated_value = ?')
                params.append(translated_value)
            
            if needs_confirmation is not None:
                update_fields.append('needs_confirmation = ?')
                params.append(needs_confirmation)
            
            if is_confirmed is not None:
                update_fields.append('is_confirmed = ?')
                params.append(is_confirmed)
            
            if translation_source is not None:
                update_fields.append('translation_source = ?')
                params.append(translation_source)
            
            if comments is not None:
                update_fields.append('comments = ?')
                params.append(comments)
            
            if not update_fields:
                return False
            
            # 添加更新时间
            update_fields.append('updated_at = ?')
            params.append(datetime.now().isoformat())
            
            # 添加ID条件
            params.append(result_id)
            
            query = f'''
                UPDATE translation_results 
                SET {', '.join(update_fields)}
                WHERE id = ?
            '''
            
            cursor.execute(query, params)
            conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            print(f"Error updating translation result: {e}")
            return False
        finally:
            conn.close()
    
    def delete_translation_result(self, result_id):
        """删除翻译结果"""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            
            cursor.execute('DELETE FROM translation_results WHERE id = ?', (result_id,))
            conn.commit()
            
            return cursor.rowcount > 0
        except Exception as e:
            print(f"Error deleting translation result: {e}")
            return False
        finally:
            conn.close()
    
    def get_translation_statistics(self, path):
        """获取翻译统计信息"""
        path_hash = hashlib.md5(path.encode()).hexdigest()
        
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            
            # 总翻译数量
            cursor.execute('SELECT COUNT(*) FROM translation_results WHERE path_hash = ?', (path_hash,))
            total_count = cursor.fetchone()[0]
            
            # 按翻译类型统计
            cursor.execute('''
                SELECT translation_type, COUNT(*) 
                FROM translation_results 
                WHERE path_hash = ? 
                GROUP BY translation_type
            ''', (path_hash,))
            type_stats = dict(cursor.fetchall())
            
            # 按翻译来源统计
            cursor.execute('''
                SELECT translation_source, COUNT(*) 
                FROM translation_results 
                WHERE path_hash = ? 
                GROUP BY translation_source
            ''', (path_hash,))
            source_stats = dict(cursor.fetchall())
            
            # 需要确认的数量
            cursor.execute('''
                SELECT COUNT(*) FROM translation_results 
                WHERE path_hash = ? AND needs_confirmation = TRUE AND is_confirmed = FALSE
            ''', (path_hash,))
            pending_confirmation = cursor.fetchone()[0]
            
            return {
                'total_count': total_count,
                'type_stats': type_stats,
                'source_stats': source_stats,
                'pending_confirmation': pending_confirmation
            }
        finally:
            conn.close()
    
    def get_all_translation_results(self, path):
        """获取指定路径的所有翻译结果"""
        path_hash = hashlib.md5(path.encode()).hexdigest()
        
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT translation_type, dataset_name, variable_name, original_value, 
                       translated_value, translation_source, comments, needs_confirmation
                FROM translation_results 
                WHERE path_hash = ?
                ORDER BY dataset_name, variable_name, original_value
            ''', (path_hash,))
            
            results = cursor.fetchall()
            return [{
                'translation_type': row[0],
                'dataset_name': row[1],
                'variable_name': row[2],
                'original_value': row[3],
                'translated_value': row[4],
                'translation_source': row[5],
                'comments': row[6],
                'needs_confirmation': bool(row[7])
            } for row in results]
        finally:
            conn.close()
    
    def perform_translation_check(self, path):
        """执行翻译核查"""
        path_hash = hashlib.md5(path.encode()).hexdigest()
        
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            
            # 仅获取needs_confirmation=1的翻译结果
            cursor.execute('''
                SELECT id, original_value, translated_value, comments, dataset_name, variable_name,
                       translation_type, translation_source, needs_confirmation, is_confirmed, confidence_score
                FROM translation_results 
                WHERE path_hash = ? AND needs_confirmation = 1
            ''', (path_hash,))
            
            results = cursor.fetchall()
            checked_items = []
            
            # 按原始值分组，用于检查相同原始值的不同翻译
            original_value_groups = {}
            for result in results:
                original_value = result[1]
                if original_value:
                    if original_value not in original_value_groups:
                        original_value_groups[original_value] = []
                    original_value_groups[original_value].append(result)
            
            # 对每个结果进行核查
            for result in results:
                result_id, original_value, translated_value, current_comments, dataset_name, variable_name, translation_type, translation_source, needs_confirmation, is_confirmed, confidence_score = result
                check_results = []
                
                # 核查规则1：中译英时，检查翻译值是否包含中文字符
                if translated_value and any('\u4e00' <= char <= '\u9fa5' for char in translated_value):
                    check_results.append('翻译值包含中文')
                
                # 核查规则2：翻译值为空
                if not translated_value or translated_value.strip() == '':
                    check_results.append('翻译值为空')
                
                # 核查规则3：相同原始值，翻译值不同且不为空
                if original_value and original_value in original_value_groups:
                    same_original_items = original_value_groups[original_value]
                    if len(same_original_items) > 1:
                        translated_values = [item[2] for item in same_original_items if item[2] and item[2].strip()]
                        unique_translations = list(set(translated_values))
                        if len(unique_translations) > 1:
                            check_results.append('相同值翻译不同，请核查')
                
                # 如果有核查结果，更新备注
                if check_results:
                    new_comments = ';'.join(check_results)
                    cursor.execute('''
                        UPDATE translation_results 
                        SET comments = ?, updated_at = ?
                        WHERE id = ?
                    ''', (new_comments, datetime.now().isoformat(), result_id))
                    
                    # 添加到核查结果列表
                    checked_items.append({
                        'id': result_id,
                        'original_value': original_value or '',
                        'translated_value': translated_value or '',
                        'comments': new_comments,
                        'dataset_name': dataset_name or '',
                        'variable_name': variable_name or '',
                        'translation_type': translation_type or '',
                        'translation_source': translation_source or '未翻译',
                        'needs_confirmation': needs_confirmation,
                        'is_confirmed': is_confirmed,
                        'confidence_score': confidence_score
                    })
            
            conn.commit()
            
            # 按备注、原始值、数据集、变量排序
            checked_items.sort(key=lambda x: (x['comments'], x['original_value'], x['dataset_name'], x['variable_name']))
            
            return checked_items
            
        except Exception as e:
            print(f"Error performing translation check: {e}")
            return []
        finally:
            conn.close()

class SASDataProcessor:
    def __init__(self):
        self.datasets = {}
        self.merged_datasets = {}
        self.hide_supp_in_preview = False
        self.translation_direction = 'zh_to_en'  # 默认中译英

    @staticmethod
    def _normalize_key_series(series: pd.Series) -> pd.Series:
        """将键列标准化为可比对的字符串表示，例如将 1.0 规范为 '1'，去除首尾空白。"""
        if series is None:
            return series
        s = series.astype(str).str.strip()
        # 尝试数值化，若为整数值则转为不带小数的字符串
        numeric = pd.to_numeric(series, errors='coerce')
        mask_num = numeric.notna()
        if mask_num.any():
            rounded = numeric.round()
            mask_int_like = mask_num & (np.abs(numeric - rounded) < 1e-9)
            s.loc[mask_int_like] = rounded.loc[mask_int_like].astype('Int64').astype(str)
        return s
        
    @staticmethod
    def _read_single_sas_file(file_path):
        """读取单个SAS文件的辅助函数，用于多线程处理"""
        try:
            dataset_name = Path(file_path).stem
            df, meta = pyreadstat.read_sas7bdat(file_path)
            return dataset_name, {
                'data': df.copy(),
                'raw_data': df.copy(),
                'meta': meta,
                'path': file_path
            }, None
        except Exception as e:
            return Path(file_path).stem, None, str(e)
    
    def read_sas_files(self, directory_path, mode='RAW', use_multithread=True, max_workers=None):
        """读取SAS数据集文件
        
        Args:
            directory_path: SAS文件目录路径
            mode: 读取模式 ('RAW' 或 'SDTM')
            use_multithread: 是否使用多线程读取
            max_workers: 最大线程数，默认为None（自动选择）
        """
        self.datasets = {}
        self.hide_supp_in_preview = (mode == 'SDTM')
        
        try:
            sas_files = glob.glob(os.path.join(directory_path, "*.sas7bdat"))
            
            if not sas_files:
                return False, "未找到SAS数据集文件"
            
            failed_files = []
            
            if use_multithread and len(sas_files) > 1:
                # 使用多线程读取
                if max_workers is None:
                    # 根据文件数量和CPU核心数自动选择线程数
                    max_workers = min(len(sas_files), os.cpu_count() or 4)
                
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    # 提交所有读取任务
                    future_to_file = {executor.submit(self._read_single_sas_file, file_path): file_path 
                                    for file_path in sas_files}
                    
                    # 收集结果
                    for future in as_completed(future_to_file):
                        file_path = future_to_file[future]
                        dataset_name, dataset_data, error = future.result()
                        
                        if error:
                            failed_files.append(f"{dataset_name}: {error}")
                        else:
                            self.datasets[dataset_name] = dataset_data
            else:
                # 单线程读取（原有逻辑）
                for file_path in sas_files:
                    dataset_name, dataset_data, error = self._read_single_sas_file(file_path)
                    if error:
                        failed_files.append(f"{dataset_name}: {error}")
                    else:
                        self.datasets[dataset_name] = dataset_data
            
            if mode == 'SDTM':
                # 仅构建预览视图，不直接改写主表
                self._build_preview_views()
            
            success_count = len(self.datasets)
            total_count = len(sas_files)
            
            if failed_files:
                error_msg = f"成功读取 {success_count}/{total_count} 个数据集。失败的文件: {'; '.join(failed_files[:3])}"
                if len(failed_files) > 3:
                    error_msg += f" 等{len(failed_files)}个文件"
                return success_count > 0, error_msg
            else:
                method = "多线程" if use_multithread and len(sas_files) > 1 else "单线程"
                return True, f"成功使用{method}读取 {success_count} 个数据集"
                
        except Exception as e:
            return False, f"读取文件失败: {str(e)}"
    
    def _process_sdtm_datasets(self):
        """兼容方法：仅为 SUPP 生成转置预览，不改写主表。"""
        for supp_name, supp_data in {n: d for n, d in self.datasets.items() if n.upper().startswith('SUPP')}.items():
            df = supp_data.get('raw_data', supp_data['data'])
            self._transpose_supp_for_display(supp_name, df)
    
    def _merge_supp_data(self, supp_name, target_name, supp_df):
        """合并SUPP数据到目标数据集（符合SDTM规范的转置逻辑）"""
        target_df = self.datasets[target_name]['data'].copy()

        # 仅处理与目标域匹配的SUPP记录（不区分大小写）
        rdomain_series = supp_df['RDOMAIN'].astype(str).str.upper() if 'RDOMAIN' in supp_df.columns else pd.Series([], dtype=str)
        filtered_supp = supp_df[rdomain_series == str(target_name).upper()].copy()
        if filtered_supp.empty:
            return

        # 规范关键列类型，避免连接时类型不一致
        for col in ['STUDYID', 'USUBJID', 'IDVAR', 'IDVARVAL', 'QNAM', 'QVAL']:
            if col in filtered_supp.columns:
                filtered_supp[col] = filtered_supp[col].astype(str)
        for col in ['STUDYID', 'USUBJID']:
            if col in target_df.columns:
                target_df[col] = target_df[col].astype(str)

        # 收集QLABEL，作为新变量的标签信息（保存在元数据中，前端可扩展使用）
        qlabel_map = (
            filtered_supp[['QNAM', 'QLABEL']]
            .dropna()
            .drop_duplicates()
            .set_index('QNAM')['QLABEL']
            .to_dict() if 'QLABEL' in filtered_supp.columns else {}
        )

        # 如果存在IDVAR，根据IDVAR进行更细粒度的转置与合并
        if 'IDVAR' in filtered_supp.columns and filtered_supp['IDVAR'].notna().any():
            updated_target = target_df
            idvar_names = filtered_supp['IDVAR'].dropna().unique()

            for idvar_name in idvar_names:
                sub = filtered_supp[filtered_supp['IDVAR'] == idvar_name].copy()
                if sub.empty:
                    continue

                # 以 STUDYID, USUBJID, IDVARVAL 为索引，将 QNAM 转列，QVAL 为值（包含所有 QNAM，如 AEYNDESC 等）
                pvt = (
                    sub.pivot_table(
                        index=['STUDYID', 'USUBJID', 'IDVARVAL'],
                        columns='QNAM',
                        values='QVAL',
                        aggfunc='first'
                    )
                    .reset_index()
                )

                # 将 IDVARVAL 列重命名为以 IDVAR 的值命名的新列（例如 AESEQ）
                pvt = pvt.rename(columns={'IDVARVAL': idvar_name})

                # 处理列名冲突：若 QNAM 与目标已有列重名，则追加后缀 _SUPP, _SUPP2...
                keep_cols = ['STUDYID', 'USUBJID', idvar_name]
                value_cols = [c for c in pvt.columns if c not in keep_cols]
                rename_map = {}
                final_value_cols = []
                for col in value_cols:
                    new_name = str(col)
                    if new_name in updated_target.columns:
                        suffix_idx = 1
                        candidate = f"{new_name}_SUPP"
                        while candidate in updated_target.columns or candidate in final_value_cols:
                            suffix_idx += 1
                            candidate = f"{new_name}_SUPP{suffix_idx}"
                        new_name = candidate
                    rename_map[col] = new_name
                    final_value_cols.append(new_name)
                if rename_map:
                    pvt = pvt.rename(columns=rename_map)
                pvt = pvt[keep_cols + final_value_cols]

                # 键列统一为字符串并标准化（处理 '1' vs '1.0'）
                for key in ['STUDYID', 'USUBJID', idvar_name]:
                    if key in updated_target.columns:
                        updated_target[key] = self._normalize_key_series(updated_target[key])
                    if key in pvt.columns:
                        pvt[key] = self._normalize_key_series(pvt[key])

                # 只有当目标数据集存在该 IDVAR 键列时才可匹配；若不存在，尝试用任意 *SEQ 列回退
                if idvar_name not in updated_target.columns:
                    fallback_idvar = None
                    for c in updated_target.columns:
                        if str(c).upper().endswith('SEQ'):
                            fallback_idvar = c
                            break
                    if fallback_idvar and fallback_idvar in updated_target.columns and idvar_name in pvt.columns:
                        # 将转置表中的 idvar 列改名为回退键
                        pvt = pvt.rename(columns={idvar_name: fallback_idvar})
                        idvar_name = fallback_idvar
                    else:
                        continue

                updated_target = updated_target.merge(
                    pvt, on=['STUDYID', 'USUBJID', idvar_name], how='left'
                )

                # 记录来源映射：这些新增列来自于 SUPP 数据集
                if final_value_cols:
                    meta = self.datasets[target_name].setdefault('extra_meta', {})
                    origin_map = meta.setdefault('supp_origin_map', {})
                    for orig_col, new_col in rename_map.items():
                        origin_map[new_col] = {
                            'supp_ds': supp_name,
                            'qnam': str(orig_col),
                            'idvar': idvar_name
                        }
                    # 对于未重名直接加入的列
                    for col in final_value_cols:
                        if col not in origin_map:
                            origin_map[col] = {
                                'supp_ds': supp_name,
                                'qnam': col,
                                'idvar': idvar_name
                            }

            self.datasets[target_name]['data'] = updated_target
        else:
            # 无IDVAR时，按 STUDYID, USUBJID 粒度转置并合并
            pvt = (
                filtered_supp.pivot_table(
                    index=['STUDYID', 'USUBJID'],
                    columns='QNAM',
                    values='QVAL',
                    aggfunc='first'
                )
                .reset_index()
            )
            # 处理列名冲突（保留所有 QNAM），并规范键列
            keep_cols = ['STUDYID', 'USUBJID']
            value_cols = [c for c in pvt.columns if c not in keep_cols]
            rename_map = {}
            final_value_cols = []
            for col in value_cols:
                new_name = str(col)
                if new_name in target_df.columns:
                    suffix_idx = 1
                    candidate = f"{new_name}_SUPP"
                    while candidate in target_df.columns or candidate in final_value_cols:
                        suffix_idx += 1
                        candidate = f"{new_name}_SUPP{suffix_idx}"
                    new_name = candidate
                rename_map[col] = new_name
                final_value_cols.append(new_name)
            if rename_map:
                pvt = pvt.rename(columns=rename_map)

            # 标准化键列以保证连接
            for key in ['STUDYID', 'USUBJID']:
                if key in target_df.columns:
                    target_df[key] = self._normalize_key_series(target_df[key])
                if key in pvt.columns:
                    pvt[key] = self._normalize_key_series(pvt[key])
            pvt = pvt[keep_cols + final_value_cols]

            merged = target_df.merge(pvt, on=['STUDYID', 'USUBJID'], how='left')
            self.datasets[target_name]['data'] = merged

            # 记录来源映射
            if final_value_cols:
                meta = self.datasets[target_name].setdefault('extra_meta', {})
                origin_map = meta.setdefault('supp_origin_map', {})
                for orig_col, new_col in rename_map.items():
                    origin_map[new_col] = {
                        'supp_ds': supp_name,
                        'qnam': str(orig_col),
                        'idvar': None
                    }
                for col in final_value_cols:
                    if col not in origin_map:
                        origin_map[col] = {
                            'supp_ds': supp_name,
                            'qnam': col,
                            'idvar': None
                        }

        # 存储QLABEL映射
        if qlabel_map:
            self.datasets[target_name].setdefault('extra_meta', {})['supp_variable_labels'] = qlabel_map

    def _transpose_supp_for_display(self, supp_name: str, supp_df: pd.DataFrame) -> None:
        """生成并缓存SUPP数据集的转置宽表，仅用于前端展示选择变量（变量名=QNAM）。"""
        if 'QNAM' not in supp_df.columns or 'QVAL' not in supp_df.columns:
            return
        base_index = ['STUDYID', 'USUBJID']
        if 'RDOMAIN' in supp_df.columns:
            base_index.append('RDOMAIN')
        if 'IDVAR' in supp_df.columns and (supp_df['IDVAR'].notna().any()):
            base_index.extend(['IDVAR', 'IDVARVAL'])

        # 统一为字符串，避免多类型导致的重复行
        for col in set(base_index + ['QNAM', 'QVAL']):
            if col in supp_df.columns:
                supp_df[col] = supp_df[col].astype(str)

        try:
            pvt = (
                supp_df.pivot_table(
                    index=[c for c in base_index if c in supp_df.columns],
                    columns='QNAM',
                    values='QVAL',
                    aggfunc='first'
                )
                .reset_index()
            )
        except Exception:
            # 回退：只用必需键
            fallback_index = [c for c in ['STUDYID', 'USUBJID'] if c in supp_df.columns]
            pvt = (
                supp_df.pivot_table(
                    index=fallback_index,
                    columns='QNAM',
                    values='QVAL',
                    aggfunc='first'
                )
                .reset_index()
            )

        # 缓存转置结果并标记仅用于预览
        self.datasets[supp_name]['pivot_for_display'] = pvt
        self.datasets[supp_name]['use_pivot_preview'] = True

    def _build_preview_views(self) -> None:
        """基于 raw_data 构建仅用于预览展示的主表视图，把 SUPP 合并进对应 RDOMAIN。"""
        # 为主表生成视图
        supp_entries = {n: d for n, d in self.datasets.items() if n.upper().startswith('SUPP')}
        for target_name, entry in self.datasets.items():
            if target_name.upper().startswith('SUPP'):
                entry.pop('data_view', None)
                continue
            target_raw = entry.get('raw_data', entry['data'])
            view_df = target_raw.copy()
            origin_map = {}

            for supp_name, supp in supp_entries.items():
                s_df = supp.get('raw_data', supp['data'])
                if 'RDOMAIN' not in s_df.columns:
                    continue
                sel = s_df[s_df['RDOMAIN'].astype(str).str.upper() == str(target_name).upper()].copy()
                if sel.empty:
                    continue
                # 先将稳定键与取值转为字符串，避免类型不一致；IDVAR/IDVARVAL 保持缺失为真正的NA
                for c in ['STUDYID','USUBJID','QNAM','QVAL']:
                    if c in sel.columns:
                        sel[c] = sel[c].astype(str)
                if 'IDVAR' in sel.columns:
                    # 将空白/"nan"/None 统一为空值
                    sel['IDVAR'] = sel['IDVAR'].where(sel['IDVAR'].notna(), pd.NA).astype(object)
                    sel['IDVAR'] = sel['IDVAR'].where(~sel['IDVAR'].astype(str).str.strip().isin(['', 'nan', 'None']), other=pd.NA)
                if 'IDVARVAL' in sel.columns:
                    sel['IDVARVAL'] = sel['IDVARVAL'].where(~sel['IDVARVAL'].astype(str).str.strip().isin(['', 'nan', 'None']), other=pd.NA)

                # 仅在存在 IDVAR 时走 IDVAR 路径；不再额外进行“无 IDVAR”回退合并，避免重复/错误合并
                processed_qnams_with_idvar: set[str] = set()
                if 'IDVAR' in sel.columns and sel['IDVAR'].notna().any():
                    for idv in [str(v) for v in sel['IDVAR'].dropna().unique()]:
                        sub = sel[sel['IDVAR']==idv]
                        if sub.empty:
                            continue
                        # 记录该分组涉及的 QNAM，供后续无 IDVAR 部分排除，避免同一 QNAM 因两类合并被加入两次
                        processed_qnams_with_idvar.update(sub['QNAM'].astype(str).unique().tolist())
                        pvt = sub.pivot_table(index=['STUDYID','USUBJID','IDVARVAL'], columns='QNAM', values='QVAL', aggfunc='first').reset_index()
                        pvt = pvt.rename(columns={'IDVARVAL': idv})
                        keep = ['STUDYID','USUBJID', idv]
                        vals = [c for c in pvt.columns if c not in keep]
                        # 重名处理
                        ren, finals = {}, []
                        for c in vals:
                            newc = str(c)
                            if newc in view_df.columns or newc in finals:
                                i=1; cand=f"{newc}_SUPP"
                                while cand in view_df.columns or cand in finals:
                                    i+=1; cand=f"{newc}_SUPP{i}"
                                newc=cand
                            ren[c]=newc; finals.append(newc)
                        if ren: pvt=pvt.rename(columns=ren)
                        # 标准化键
                        for k in ['STUDYID','USUBJID', idv]:
                            if k in view_df.columns: view_df[k]=self._normalize_key_series(view_df[k])
                            if k in pvt.columns: pvt[k]=self._normalize_key_series(pvt[k])
                        join_id = idv if idv in view_df.columns else None
                        if not join_id:
                            # 回退 *SEQ
                            for c in view_df.columns:
                                if str(c).upper().endswith('SEQ'):
                                    if idv in pvt.columns:
                                        pvt=pvt.rename(columns={idv:c})
                                    join_id=c; break
                        if not join_id: 
                            continue
                        view_df = view_df.merge(pvt[['STUDYID','USUBJID', join_id] + finals], left_on=['STUDYID','USUBJID', join_id], right_on=['STUDYID','USUBJID', join_id], how='left')
                        for oc, nc in ren.items(): origin_map[nc]={'supp_ds':supp_name,'qnam':str(oc),'idvar':idv}
                        for nc in finals:
                            if nc not in origin_map: origin_map[nc]={'supp_ds':supp_name,'qnam':nc,'idvar':idv}
                # 仍需处理 IDVAR 为空的行：按 STUDYID/USUBJID 合并
                # 若完全不存在 IDVAR 列，才按 STUDYID/USUBJID 合并（兼容极少量数据集）
                elif 'IDVAR' not in sel.columns:
                    pvt = sel.pivot_table(index=['STUDYID','USUBJID'], columns='QNAM', values='QVAL', aggfunc='first').reset_index()
                    keep=['STUDYID','USUBJID']
                    vals=[c for c in pvt.columns if c not in keep]
                    ren, finals = {}, []
                    for c in vals:
                        newc=str(c)
                        if newc in view_df.columns or newc in finals:
                            i=1; cand=f"{newc}_SUPP"
                            while cand in view_df.columns or cand in finals:
                                i+=1; cand=f"{newc}_SUPP{i}"
                            newc=cand
                        ren[c]=newc; finals.append(newc)
                    if ren: pvt=pvt.rename(columns=ren)
                    for k in ['STUDYID','USUBJID']:
                        if k in view_df.columns: view_df[k]=self._normalize_key_series(view_df[k])
                        if k in pvt.columns: pvt[k]=self._normalize_key_series(pvt[k])
                    view_df=view_df.merge(pvt[keep+finals], on=['STUDYID','USUBJID'], how='left')
                    for oc,nc in ren.items(): origin_map[nc]={'supp_ds':supp_name,'qnam':str(oc),'idvar':None}
                    for nc in finals:
                        if nc not in origin_map: origin_map[nc]={'supp_ds':supp_name,'qnam':nc,'idvar':None}
                else:
                    pvt = sel.pivot_table(index=['STUDYID','USUBJID'], columns='QNAM', values='QVAL', aggfunc='first').reset_index()
                    keep=['STUDYID','USUBJID']
                    vals=[c for c in pvt.columns if c not in keep]
                    ren, finals = {}, []
                    for c in vals:
                        newc=str(c)
                        if newc in view_df.columns or newc in finals:
                            i=1; cand=f"{newc}_SUPP"
                            while cand in view_df.columns or cand in finals:
                                i+=1; cand=f"{newc}_SUPP{i}"
                            newc=cand
                        ren[c]=newc; finals.append(newc)
                    if ren: pvt=pvt.rename(columns=ren)
                    for k in ['STUDYID','USUBJID']:
                        if k in view_df.columns: view_df[k]=self._normalize_key_series(view_df[k])
                        if k in pvt.columns: pvt[k]=self._normalize_key_series(pvt[k])
                    view_df=view_df.merge(pvt[keep+finals], on=['STUDYID','USUBJID'], how='left')
                    for oc,nc in ren.items(): origin_map[nc]={'supp_ds':supp_name,'qnam':str(oc),'idvar':None}
                    for nc in finals:
                        if nc not in origin_map: origin_map[nc]={'supp_ds':supp_name,'qnam':nc,'idvar':None}

            entry['data_view']=view_df
            entry.setdefault('extra_meta',{})['preview_origin_map']=origin_map
    
    def get_dataset_preview(self, dataset_name, limit=10, offset=0):
        """获取数据集预览，支持分页 limit/offset，limit<=0 或 None 表示全量。SUPP使用转置预览。"""
        if dataset_name in self.datasets:
            entry = self.datasets[dataset_name]
            if self.hide_supp_in_preview:
                if dataset_name.upper().startswith('SUPP'):
                    return {
                        'columns': ['MESSAGE'],
                        'data': [{'MESSAGE': 'SUPP 数据已合并至对应 RDOMAIN 展示'}],
                        'total_rows': 1,
                        'dataset_name': dataset_name
                    }
                df = entry.get('data_view', entry['data'])
            else:
                df = entry['data']
                if dataset_name.upper().startswith('SUPP') and entry.get('use_pivot_preview') and 'pivot_for_display' in entry:
                    df = entry['pivot_for_display']
            total_rows = len(df)
            # 为确保JSON有效性，将NaN/NaT/Inf等值转换为null，并规范时间格式
            if limit is None or limit <= 0:
                sample = df.copy()
            else:
                start = max(0, int(offset))
                end = max(start, start + int(limit))
                sample = df.iloc[start:end].copy()
            # 修复Pandas未来版本兼容性警告 - 使用更安全的方法处理无穷值
            for col in sample.select_dtypes(include=[np.number]).columns:
                sample[col] = sample[col].where(~np.isinf(sample[col]), pd.NA)
            # 使用pandas内置to_json处理缺失值为null，再反序列化回Python对象
            records_json = sample.to_json(orient='records', date_format='iso', force_ascii=False)
            safe_records = json.loads(records_json)

            return {
                'columns': list(df.columns),
                'data': safe_records,
                'total_rows': total_rows,
                'dataset_name': dataset_name
            }
        return None
    
    def get_all_datasets_info(self):
        """获取所有数据集的基本信息。SUPP显示使用转置列做选择；在SDTM模式下不单独展示SUPP。"""
        info = {}
        for name, data in self.datasets.items():
            if self.hide_supp_in_preview and name.upper().startswith('SUPP'):
                # 在SDTM模式下不单独展示SUPP数据集
                continue
            if self.hide_supp_in_preview:
                preview_df = data.get('data_view', data['data'])
            else:
                preview_df = data['data']
                if name.upper().startswith('SUPP') and data.get('use_pivot_preview') and 'pivot_for_display' in data:
                    preview_df = data['pivot_for_display']
            # 供前端变量选择用的列（SUPP* 仅暴露转置后的 QNAM 列）
            selectable_columns = list(preview_df.columns)
            if name.upper().startswith('SUPP'):
                exclude_cols = {
                    'STUDYID', 'USUBJID', 'RDOMAIN', 'IDVAR', 'IDVARVAL',
                    'QNAM', 'QVAL', 'QLABEL', 'QORIG'
                }
                selectable_columns = [c for c in preview_df.columns if str(c).upper() not in exclude_cols]

            # 计算从SUPP合入列的失败情况（在目标主表中：值全为空视为失败）
            extra_meta = data.get('extra_meta', {}) if isinstance(data, dict) else {}
            origin_map = (extra_meta.get('preview_origin_map', {}) if self.hide_supp_in_preview else extra_meta.get('supp_origin_map', {})) if isinstance(extra_meta, dict) else {}
            supp_failed_columns = []
            supp_origin_columns = list(origin_map.keys()) if isinstance(origin_map, dict) else []
            if isinstance(origin_map, dict) and len(origin_map) > 0:
                total_rows = len(preview_df)
                for col, meta in origin_map.items():
                    if col in preview_df.columns:
                        non_null = int(preview_df[col].notna().sum())
                        if total_rows > 0 and non_null == 0:
                            supp_failed_columns.append({
                                'column': col,
                                'non_null': non_null,
                                'total': total_rows,
                                'supp_ds': meta.get('supp_ds'),
                                'qnam': meta.get('qnam'),
                                'idvar': meta.get('idvar')
                            })

            info[name] = {
                'rows': len(preview_df),
                'columns': len(preview_df.columns),
                'column_names': list(preview_df.columns),
                'selectable_columns': selectable_columns,
                'supp_origin_columns': supp_origin_columns,
                'supp_failed_columns': supp_failed_columns,
                # 暴露列→来源明细，供前端在选择源变量时映射回 SUPP.QNAM
                'supp_origin_detail': origin_map
            }
        return info
    
    def merge_variables(self, merge_config):
        """根据配置合并变量。
        支持两种配置格式：
        1) 旧格式：{'dataset': 'CM', 'target': 'NEW', 'sources': ['A','B']}
        2) 新格式：{'target': {'dataset': 'CM', 'column': 'NEW'}, 'sources': [{'dataset':'CM','column':'A'}, ...]}
        当来源变量来自其他数据集时，将按公共键对齐（默认使用 STUDYID、USUBJID 及双方共同的 *SEQ 键）。
        仅在目标数据集内删除被合并的源变量。
        """
        try:
            for config in merge_config:
                # 解析配置（兼容旧版）
                if isinstance(config.get('target'), dict):
                    target_dataset = config['target'].get('dataset')
                    target_var = config['target'].get('column')
                    sources_desc = config.get('sources', [])  # list of {dataset,column}
                else:
                    target_dataset = config.get('dataset')
                    target_var = config.get('target')
                    sources_desc = [{'dataset': target_dataset, 'column': col} for col in config.get('sources', [])]

                if not target_dataset or not target_var or target_dataset not in self.datasets:
                    # 忽略无效配置
                    continue

                # 对主表目标：一律在 raw_data 上操作
                target_df = self.datasets[target_dataset].get('raw_data', self.datasets[target_dataset]['data']).copy()
                # 规范化主表基本键
                for kc in ['STUDYID', 'USUBJID']:
                    if kc in target_df.columns:
                        target_df[kc] = self._normalize_key_series(target_df[kc])

                # 情况A：目标在SUPP中（target_var 为 QNAM）。合并到 SUPP 的 QVAL，并删除源 SUPP QNAM 行
                if target_dataset.upper().startswith('SUPP'):
                    supp_entry = self.datasets[target_dataset]
                    # 始终以 raw_data 为基准进行变更，这样预览重建才能完全生效
                    supp_df = supp_entry.get('raw_data', supp_entry['data']).copy()

                    # 目标行
                    if 'QNAM' not in supp_df.columns or 'QVAL' not in supp_df.columns:
                        continue
                    target_rows = supp_df[supp_df['QNAM'] == target_var].copy()
                    if target_rows.empty:
                        continue

                    # 键集合
                    key_cols = [c for c in ['STUDYID', 'USUBJID', 'RDOMAIN', 'IDVAR', 'IDVARVAL'] if c in supp_df.columns]
                    # 规范化键
                    for kc in ['STUDYID', 'USUBJID', 'IDVARVAL']:
                        if kc in target_rows.columns:
                            target_rows[kc] = self._normalize_key_series(target_rows[kc])

                    # 为每个来源构建按键对齐的 Series；保留原有 QVAL 作为首列
                    aligned_series = [target_rows['QVAL']]
                    supp_to_drop_map: dict[str, set] = {}
                    for src in sources_desc:
                        src_ds = src.get('dataset')
                        src_col = src.get('column')
                        if not src_ds or not src_col or src_ds not in self.datasets:
                            aligned_series.append(pd.Series([pd.NA] * len(target_rows), index=target_rows.index))
                            continue
                        if src_ds.upper().startswith('SUPP'):
                            s_entry = self.datasets[src_ds]
                            s_df = s_entry.get('raw_data', s_entry['data']).copy()
                            if 'QNAM' not in s_df.columns or 'QVAL' not in s_df.columns:
                                aligned_series.append(pd.Series([pd.NA] * len(target_rows), index=target_rows.index))
                                continue
                            rows = s_df[s_df['QNAM'] == src_col].copy()
                            if rows.empty:
                                aligned_series.append(pd.Series([pd.NA] * len(target_rows), index=target_rows.index))
                                continue
                            # 规范化键
                            for kc in ['STUDYID', 'USUBJID', 'IDVARVAL']:
                                if kc in rows.columns:
                                    rows[kc] = self._normalize_key_series(rows[kc])
                            # 选择连接键
                            join_keys = [k for k in ['STUDYID', 'USUBJID'] if k in target_rows.columns and k in rows.columns]
                            if 'IDVAR' in target_rows.columns and 'IDVAR' in rows.columns and 'IDVARVAL' in target_rows.columns and 'IDVARVAL' in rows.columns:
                                join_keys += ['IDVAR', 'IDVARVAL']
                            elif 'IDVARVAL' in target_rows.columns and 'IDVARVAL' in rows.columns:
                                join_keys += ['IDVARVAL']
                            # 合并
                            slim = rows[join_keys + ['QVAL']].drop_duplicates(subset=join_keys)
                            merged = target_rows[join_keys].merge(slim, on=join_keys, how='left')
                            ser = merged['QVAL'] if 'QVAL' in merged.columns else pd.Series([pd.NA] * len(target_rows))
                            ser.index = target_rows.index
                            aligned_series.append(ser)
                            # 标记删除来源 QNAM
                            supp_to_drop_map.setdefault(src_ds, set()).add(src_col)
                        else:
                            # 来源为主表：按 STUDYID/USUBJID 及 IDVAR 指定的键对齐
                            s_df = self.datasets[src_ds]['data']
                            # 连接键
                            join_keys = [k for k in ['STUDYID', 'USUBJID'] if k in target_rows.columns and k in s_df.columns]
                            if 'IDVAR' in target_rows.columns:
                                idvar_values = target_rows['IDVAR'].dropna().unique().tolist()
                                chosen = None
                                for cand in idvar_values:
                                    if cand in s_df.columns and 'IDVARVAL' in target_rows.columns:
                                        chosen = cand
                                        break
                                if chosen:
                                    # 用 chosen 与 IDVARVAL 对齐
                                    tmp_left = target_rows[join_keys + ['IDVARVAL']].copy()
                                    tmp_left = tmp_left.rename(columns={'IDVARVAL': chosen})
                                    for kc in [chosen]:
                                        if kc in s_df.columns:
                                            s_df[kc] = self._normalize_key_series(s_df[kc])
                                        if kc in tmp_left.columns:
                                            tmp_left[kc] = self._normalize_key_series(tmp_left[kc])
                                    merged = tmp_left.merge(s_df[join_keys + [chosen, src_col]] if src_col in s_df.columns else tmp_left.assign(**{src_col: pd.NA}),
                                                           on=join_keys + [chosen], how='left')
                                    ser = merged[src_col] if src_col in merged.columns else pd.Series([pd.NA] * len(target_rows))
                                    ser.index = target_rows.index
                                    aligned_series.append(ser)
                                    continue
                            # 退化为只按 STUDYID/USUBJID
                            merged = target_rows[join_keys].merge(s_df[join_keys + [src_col]] if src_col in s_df.columns else target_rows.assign(**{src_col: pd.NA}),
                                                               on=join_keys, how='left')
                            ser = merged[src_col] if src_col in merged.columns else pd.Series([pd.NA] * len(target_rows))
                            ser.index = target_rows.index
                            aligned_series.append(ser)

                    # 拼接并写回 QVAL
                    concat_df = pd.DataFrame({f'_s{i}': s for i, s in enumerate(aligned_series)})
                    def _join_parts(row):
                        parts = []
                        for v in row.values.tolist():
                            if pd.notna(v):
                                sv = str(v)
                                if sv.lower() != 'nan' and sv != '':
                                    parts.append(sv)
                        # 根据翻译方向决定是否使用空格间隔
                        separator = ' ' if self.translation_direction == 'en_to_zh' else ''
                        return separator.join(parts)
                    target_rows['QVAL'] = concat_df.apply(_join_parts, axis=1).values
                    # 写回原表
                    supp_df.loc[target_rows.index, 'QVAL'] = target_rows['QVAL']
                    # 回写 raw 与 data
                    self.datasets[target_dataset]['raw_data'] = supp_df.copy()
                    self.datasets[target_dataset]['data'] = supp_df.copy()

                    # 删除源 SUPP QNAM
                    for s_ds, qnams in supp_to_drop_map.items():
                        e = self.datasets.get(s_ds)
                        if not e:
                            continue
                        src_df = e.get('raw_data', e['data']).copy()
                        if 'QNAM' in src_df.columns:
                            filtered = src_df[~src_df['QNAM'].isin(list(qnams))].copy()
                            e['raw_data'] = filtered.copy()
                            e['data'] = filtered.copy()
                            # 更新SUPP的转置供选择器使用
                            self._transpose_supp_for_display(s_ds, filtered)

                    # 关键：重新映射当前所有 SUPP → 主表 的视图，保持最初合并规则与键不变
                    self._build_preview_views()

                    # 进入下一条配置
                    continue

                # 预备：目标列若不存在则创建空字符串列
                if target_var not in target_df.columns:
                    target_df[target_var] = ''

                # 组装每个来源列，按顺序对齐到目标数据集；保留原有目标列值作为首列
                aligned_source_series: list[pd.Series] = [target_df[target_var]]

                # 记录需要从SUPP原始结构中删除的QNAM
                supp_to_drop_map: dict[str, set] = {}

                for src in sources_desc:
                    src_dataset = src.get('dataset')
                    src_col = src.get('column')
                    if not src_dataset or not src_col or src_dataset not in self.datasets:
                        # 无效来源，追加空系列
                        aligned_source_series.append(pd.Series([pd.NA] * len(target_df), index=target_df.index))
                        continue

                    source_entry = self.datasets[src_dataset]
                    source_df = source_entry.get('raw_data', source_entry['data']).copy()

                    if src_dataset == target_dataset:
                        # 同表直接取列
                        series = source_df[src_col] if src_col in source_df.columns else pd.Series([pd.NA]*len(target_df), index=target_df.index)
                        aligned_source_series.append(series)
                        continue

                    # 跨表：基于公共键对齐
                    # 选择键：STUDYID、USUBJID，以及共同存在、名字以SEQ结尾的列（如 CMSEQ）
                    common_keys = [k for k in ['STUDYID', 'USUBJID'] if k in target_df.columns and k in source_df.columns]
                    seq_keys = [c for c in target_df.columns.intersection(source_df.columns) if str(c).upper().endswith('SEQ')]
                    join_keys = common_keys + (seq_keys[:1] if seq_keys else [])

                    # 对 SUPP* 特例：来源的“列名”实际上是 QNAM；需从原始SUPP结构中抽取 QVAL 并按键对齐
                    if src_dataset.upper().startswith('SUPP'):
                        qnam = src_col
                        if 'QNAM' in source_df.columns and 'QVAL' in source_df.columns:
                            rows = source_df[source_df['QNAM'] == qnam].copy()
                            # 若 SUPP 中有 RDOMAIN，则限定为与目标主表一致
                            if 'RDOMAIN' in rows.columns:
                                rows = rows[rows['RDOMAIN'].astype(str).str.upper() == str(target_dataset).upper()].copy()
                            if not rows.empty:
                                # 标记删除该QNAM
                                supp_to_drop_map.setdefault(src_dataset, set()).add(qnam)
                                # 若有IDVAR，则优先用 IDVAR 与 IDVARVAL 指向的目标键列（如 CMSEQ）
                                chosen_idvar = None
                                if 'IDVAR' in rows.columns and rows['IDVAR'].notna().any():
                                    for cand in rows['IDVAR'].dropna().unique().tolist():
                                        if cand in target_df.columns:
                                            chosen_idvar = cand
                                            break
                                if chosen_idvar and 'IDVARVAL' in rows.columns:
                                    eff = rows[['STUDYID','USUBJID','IDVARVAL','QVAL']].copy()
                                    eff = eff.rename(columns={'IDVARVAL': chosen_idvar, 'QVAL': qnam})
                                    # 规范化对齐键（STUDYID/USUBJID/CMSEQ 等）
                                    for k in ['STUDYID','USUBJID']:
                                        if k in eff.columns:
                                            eff[k] = self._normalize_key_series(eff[k])
                                    if chosen_idvar in target_df.columns:
                                        target_df[chosen_idvar] = self._normalize_key_series(target_df[chosen_idvar])
                                    if chosen_idvar in eff.columns:
                                        eff[chosen_idvar] = self._normalize_key_series(eff[chosen_idvar])
                                    eff = eff.drop_duplicates(subset=['STUDYID','USUBJID', chosen_idvar])
                                    left_keys = [k for k in ['STUDYID','USUBJID', chosen_idvar] if k in target_df.columns and k in eff.columns]
                                    merged = target_df[left_keys].merge(eff, on=left_keys, how='left')
                                    series = merged[qnam] if qnam in merged.columns else pd.Series([pd.NA]*len(target_df))
                                    aligned_source_series.append(series)
                                    continue
                                else:
                                    eff = rows[['STUDYID','USUBJID','QVAL']].copy()
                                    eff = eff.rename(columns={'QVAL': qnam})
                                    for k in ['STUDYID','USUBJID']:
                                        if k in eff.columns:
                                            eff[k] = self._normalize_key_series(eff[k])
                                    eff = eff.drop_duplicates(subset=['STUDYID','USUBJID'])
                                    left_keys = [k for k in ['STUDYID','USUBJID'] if k in target_df.columns and k in eff.columns]
                                    merged = target_df[left_keys].merge(eff, on=left_keys, how='left')
                                    series = merged[qnam] if qnam in merged.columns else pd.Series([pd.NA]*len(target_df))
                                    aligned_source_series.append(series)
                                    continue

                    if not join_keys:
                        # 无法对齐，追加空
                        aligned_source_series.append(pd.Series([pd.NA]*len(target_df), index=target_df.index))
                        continue

                    # 多对一：先根据 join_keys 聚合取第一个
                    src_slim = source_df[join_keys + ([src_col] if src_col in source_df.columns else [])].copy() if src_col in source_df.columns else None
                    if src_slim is None:
                        aligned_source_series.append(pd.Series([pd.NA]*len(target_df), index=target_df.index))
                        continue
                    src_slim = src_slim.drop_duplicates(subset=join_keys)

                    left_keys = [k for k in join_keys if k in target_df.columns]
                    merged = target_df[left_keys].merge(src_slim, on=left_keys, how='left')
                    series = merged[src_col] if src_col in merged.columns else pd.Series([pd.NA]*len(target_df))
                    aligned_source_series.append(series)

                # 生成目标列：按顺序拼接非空值
                aligned_frame = pd.DataFrame({f'_s{i}': s for i, s in enumerate(aligned_source_series)})

                def concat_row(row):
                    parts = []
                    for val in row.values.tolist():
                        if pd.notna(val):
                            sval = str(val)
                            if sval.lower() != 'nan' and sval != '':
                                parts.append(sval)
                    # 根据翻译方向决定是否使用空格间隔
                    separator = ' ' if self.translation_direction == 'en_to_zh' else ''
                    return separator.join(parts)

                target_df[target_var] = aligned_frame.apply(concat_row, axis=1)

                # 在目标数据集中删除与目标同表的源列
                same_table_source_cols = [s.get('column') for s in sources_desc if s.get('dataset') == target_dataset]
                if same_table_source_cols:
                    target_df.drop(columns=same_table_source_cols, inplace=True, errors='ignore')

                # 回写目标（raw 与 data）
                self.datasets[target_dataset]['raw_data'] = target_df.copy()
                self.datasets[target_dataset]['data'] = target_df.copy()

                # 从各SUPP原始结构中删除已合并的 QNAM，并同步更新其转置预览
                for supp_ds, qnams in supp_to_drop_map.items():
                    entry = self.datasets.get(supp_ds)
                    if not entry:
                        continue
                    raw = entry.get('raw_data', entry['data']).copy()
                    if 'QNAM' in raw.columns:
                        filtered = raw[~raw['QNAM'].isin(list(qnams))].copy()
                        entry['raw_data'] = filtered.copy()
                        entry['data'] = filtered.copy()
                        # 重新生成预览转置
                        self._transpose_supp_for_display(supp_ds, filtered)

                # 重新构建主表预览视图
                self._build_preview_views()

            return True, "变量合并成功"
        except Exception as e:
            # 将异常同时打印到控制台，便于调试
            print('merge_variables error:', e)
            return False, f"变量合并失败: {str(e)}"

    def _refresh_main_from_supp(self, supp_name: str, affected_qnams: set) -> None:
        """将指定 SUPP 的受影响 QNAM 刷新到各主表已存在的映射列（覆盖写入）。"""
        supp_entry = self.datasets.get(supp_name)
        if not supp_entry:
            return
        supp_df = supp_entry['data']
        if 'QNAM' not in supp_df.columns or 'QVAL' not in supp_df.columns:
            return
        # 准备规范化键
        for c in ['STUDYID', 'USUBJID', 'IDVARVAL']:
            if c in supp_df.columns:
                supp_df[c] = self._normalize_key_series(supp_df[c])
        for ds_name, entry in self.datasets.items():
            if ds_name.upper().startswith('SUPP'):
                continue
            meta = entry.setdefault('extra_meta', {})
            origin_map = meta.get('supp_origin_map', {})
            if not origin_map:
                continue
            target_df = entry['data']
            for col, m in origin_map.items():
                if m.get('supp_ds') != supp_name:
                    continue
                qn = str(m.get('qnam'))
                if qn not in affected_qnams:
                    # 仅刷新受影响的 QNAM，避免不必要开销
                    continue
                # 取 SUPP 中该 QNAM 的值
                rows = supp_df[supp_df['QNAM'] == qn].copy()
                if rows.empty:
                    target_df[col] = pd.NA
                    continue
                # 键连接：优先 STUDYID/USUBJID + IDVAR 指定的键
                join_keys = [k for k in ['STUDYID', 'USUBJID'] if k in target_df.columns and k in rows.columns]
                if m.get('idvar') and m['idvar'] in target_df.columns and 'IDVARVAL' in rows.columns:
                    # 用 idvar 与 IDVARVAL 对齐
                    tmp_left = target_df[join_keys + [m['idvar']]].copy()
                    # 规范化
                    tmp_left[m['idvar']] = self._normalize_key_series(tmp_left[m['idvar']])
                    merged = tmp_left.merge(rows[join_keys + ['IDVARVAL','QVAL']], left_on=join_keys + [m['idvar']], right_on=join_keys + ['IDVARVAL'], how='left')
                    vals = merged['QVAL'] if 'QVAL' in merged.columns else pd.Series([pd.NA] * len(target_df))
                else:
                    merged = target_df[join_keys].merge(rows[join_keys + ['QVAL']], on=join_keys, how='left')
                    vals = merged['QVAL'] if 'QVAL' in merged.columns else pd.Series([pd.NA] * len(target_df))
                target_df[col] = vals.reset_index(drop=True)
            entry['data'] = target_df

# 全局处理器实例
processor = SASDataProcessor()
db_manager = DatabaseManager()

@app.route('/')
def index():
    return render_template('index.html')



@app.route('/read_datasets', methods=['POST'])
def read_datasets():
    data = request.get_json()
    directory_path = data.get('path', '')
    mode = data.get('mode', 'RAW')
    translation_direction = data.get('translation_direction', 'zh_to_en')
    
    if not directory_path or not os.path.exists(directory_path):
        return jsonify({'success': False, 'message': '路径不存在或为空'})
    
    # 设置翻译方向
    processor.translation_direction = translation_direction
    success, message = processor.read_sas_files(directory_path, mode)
    
    if success:
        datasets_info = processor.get_all_datasets_info()
        return jsonify({
            'success': True,
            'message': message,
            'datasets': datasets_info
        })
    else:
        return jsonify({'success': False, 'message': message})

@app.route('/get_dataset/<dataset_name>')
def get_dataset(dataset_name):
    # 支持查询参数：?all=1 或 ?page=1&page_size=100 或 ?limit=100&offset=0
    all_flag = request.args.get('all', default='0')
    if all_flag in ('1', 'true', 'True'):
        limit = None
        offset = 0
    else:
        # 优先支持 page/page_size
        page = request.args.get('page', default=None, type=int)
        page_size = request.args.get('page_size', default=None, type=int)
        if page is not None and page_size is not None:
            page = max(1, page)
            page_size = max(1, page_size)
            limit = page_size
            offset = (page - 1) * page_size
        else:
            # 兼容 limit/offset
            limit = request.args.get('limit', default=100, type=int)
            offset = request.args.get('offset', default=0, type=int)

    preview = processor.get_dataset_preview(dataset_name, limit=limit, offset=offset)
    if preview:
        # 附加分页元数据（当非全量时）
        if limit is not None and limit > 0:
            total_rows = preview['total_rows']
            current_page = (offset // limit) + 1
            total_pages = (total_rows + limit - 1) // limit if limit else 1
            preview.update({
                'page': current_page,
                'page_size': limit,
                'total_pages': total_pages
            })
        return jsonify(preview)
    else:
        return jsonify({'error': '数据集未找到'}), 404

@app.route('/get_source_variables/<main_dataset>', methods=['GET'])
def get_source_variables(main_dataset):
    """获取指定主数据集对应的SUPP数据集中可用的源变量"""
    try:
        if main_dataset not in processor.datasets:
            return jsonify({'error': f'数据集 {main_dataset} 不存在'})
        
        # 查找对应的SUPP数据集
        supp_dataset_name = None
        possible_supp_names = [
            f'SUPP{main_dataset}',
            f'SUPP{main_dataset.lower()}',
            f'supp{main_dataset}',
            f'supp{main_dataset.lower()}'
        ]
        
        for supp_name in possible_supp_names:
            if supp_name in processor.datasets:
                supp_dataset_name = supp_name
                break
        
        if not supp_dataset_name:
            # 搜索包含SUPP且包含主数据集名的数据集
            import re
            supp_pattern = re.compile(f'supp.*{re.escape(main_dataset)}', re.IGNORECASE)
            for dataset_name in processor.datasets.keys():
                if supp_pattern.search(dataset_name):
                    supp_dataset_name = dataset_name
                    break
        
        if not supp_dataset_name:
            return jsonify({
                'source_variables': [],
                'message': f'未找到与 {main_dataset} 对应的SUPP数据集'
            })
        
        # 获取SUPP数据集信息
        supp_data = processor.datasets.get(supp_dataset_name)
        if not supp_data:
            return jsonify({'error': f'SUPP数据集 {supp_dataset_name} 数据为空'})
        
        # 优先从原始数据获取QNAM值，如果没有则从处理后的数据获取
        raw_df = supp_data.get('raw_data')
        df = raw_df if raw_df is not None else supp_data.get('data')
        
        if df is None or df.empty:
            return jsonify({'error': f'SUPP数据集 {supp_dataset_name} 无有效数据'})
        
        # 从QNAM列获取唯一值作为源变量（这是SDTM SUPP数据集的标准结构）
        if 'QNAM' in df.columns:
            # 过滤与主数据集相关的SUPP记录
            filtered_df = df
            if 'RDOMAIN' in df.columns:
                # 只保留与主数据集RDOMAIN匹配的记录
                filtered_df = df[df['RDOMAIN'].astype(str).str.upper() == main_dataset.upper()]
            
            if not filtered_df.empty:
                # 从QNAM列获取唯一值作为源变量
                qnam_values = filtered_df['QNAM'].dropna().unique().tolist()
                available_columns = [str(val).strip() for val in qnam_values 
                                   if str(val).strip() not in ['', 'nan', 'None', 'NaN']]
            else:
                available_columns = []
        else:
            # 如果没有QNAM列，尝试从转置后的列名获取
            base_columns = {'STUDYID', 'USUBJID', 'RDOMAIN', 'IDVAR', 'IDVARVAL', 'QNAM', 'QVAL', 'QLABEL', 'QORIG'}
            available_columns = [col for col in df.columns if col not in base_columns and not str(col).startswith('_')]
        
        return jsonify({
            'source_variables': available_columns,
            'supp_dataset': supp_dataset_name,
            'message': f'找到 {len(available_columns)} 个可用源变量'
        })
        
    except Exception as e:
        return jsonify({'error': f'获取源变量失败: {str(e)}'})

@app.route('/merge_variables', methods=['POST'])
def merge_variables():
    data = request.get_json()
    merge_config = data.get('config', [])
    translation_direction = data.get('translation_direction', 'zh_to_en')
    
    # 设置翻译方向
    processor.translation_direction = translation_direction
    success, message = processor.merge_variables(merge_config)
    
    if success:
        datasets_info = processor.get_all_datasets_info()
        resp = jsonify({
            'success': True,
            'message': message,
            'datasets': datasets_info
        })
        # 明确禁用缓存，确保前端立即拿到最新视图
        resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
        resp.headers['Pragma'] = 'no-cache'
        return resp
    else:
        return jsonify({'success': False, 'message': message})

@app.route('/execute_merge', methods=['POST'])
def execute_merge():
    """执行合并配置表的逻辑"""
    try:
        data = request.get_json()
        merge_config = data.get('merge_config', [])
        translation_config = data.get('translation_config', {})
        
        # 设置翻译方向
        translation_direction = translation_config.get('translation_direction', 'zh_to_en')
        processor.translation_direction = translation_direction
        
        # 执行合并
        success, message = processor.merge_variables(merge_config)
        
        if success:
            # 获取处理后的数据集信息
            datasets_info = processor.get_all_datasets_info()
            return jsonify({
                'success': True,
                'message': message,
                'data': datasets_info
            })
        else:
            return jsonify({'success': False, 'message': message}), 500
            
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/save_merge_config', methods=['POST'])
def save_merge_config():
    try:
        data = request.get_json()
        success = db_manager.save_merge_config(
            path=data.get('path'),
            translation_direction=data.get('translation_direction'),
            configs=data.get('config')
        )
        
        if success:
            return jsonify({'success': True, 'message': '合并配置保存成功'})
        else:
            return jsonify({'success': False, 'message': '保存失败'}), 500
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/load_merge_config', methods=['GET'])
def load_merge_config():
    """加载合并配置"""
    try:
        path = request.args.get('path')
        if not path:
            return jsonify({'success': False, 'message': '缺少path参数'}), 400
        
        config = db_manager.get_merge_config(path)
        
        if config:
            return jsonify({'success': True, 'config': config})
        else:
            return jsonify({'success': False, 'message': '未找到配置'}), 404
            
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

# 数据库管理API
@app.route('/api/save_mapping_config', methods=['POST'])
def save_mapping_config():
    try:
        data = request.get_json()
        success = db_manager.save_mapping_config(
            path=data.get('path'),
            mode=data.get('mode'),
            translation_direction=data.get('translation_direction'),
            configs=data.get('configs'),
            name=data.get('name')
        )
        
        if success:
            return jsonify({'success': True, 'message': '映射配置保存成功'})
        else:
            return jsonify({'success': False, 'message': '保存失败'}), 500
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/init_database', methods=['POST'])
def init_database():
    try:
        db_manager.init_database()
        return jsonify({'success': True, 'message': '数据库初始化成功'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/database_tables', methods=['GET'])
def get_database_tables():
    try:
        tables = db_manager.get_database_tables()
        return jsonify({'success': True, 'tables': tables})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/create_table', methods=['POST'])
def create_table():
    try:
        data = request.get_json()
        success = db_manager.create_table(
            category=data.get('category'),
            name=data.get('name'),
            description=data.get('description', '')
        )
        
        if success:
            return jsonify({'success': True, 'message': '表创建成功'})
        else:
            return jsonify({'success': False, 'message': '表名已存在或创建失败'}), 400
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/delete_table', methods=['POST'])
def delete_table():
    try:
        data = request.get_json()
        success = db_manager.delete_table(
            category=data.get('category'),
            table_name=data.get('table_name')
        )
        
        if success:
            return jsonify({'success': True, 'message': '表删除成功'})
        else:
            return jsonify({'success': False, 'message': '删除失败'}), 500
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/upload_database_file', methods=['POST'])
def upload_database_file():
    try:
        category = request.form.get('category')
        table_name = request.form.get('table_name')
        description = request.form.get('description', '')
        file = request.files.get('file')
        
        if not file or not table_name:
            return jsonify({'success': False, 'message': '缺少必要参数'}), 400
        
        # 创建数据目录
        data_dir = Path('data')
        data_dir.mkdir(exist_ok=True)
        
        # 保存文件
        file_path = data_dir / f"{category}_{table_name}_{file.filename}"
        file.save(str(file_path))
        
        # 读取文件并获取记录数
        record_count = 0
        try:
            if file.filename.endswith('.xlsx') or file.filename.endswith('.xls'):
                df = pd.read_excel(file_path)
                record_count = len(df)
            elif file.filename.endswith('.csv'):
                df = pd.read_csv(file_path)
                record_count = len(df)
            elif file.filename.endswith('.asc'):
                # ASC文件需要特殊处理
                with open(file_path, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                    record_count = len([line for line in lines if line.strip()])
        except Exception as e:
            print(f"Error reading file: {e}")
        
        # 保存到数据库
        conn = sqlite3.connect(db_manager.db_path)
        try:
            cursor = conn.cursor()
            table = f"{category}_tables"
            cursor.execute(f'''
                INSERT OR REPLACE INTO {table} (name, description, file_path, record_count)
                VALUES (?, ?, ?, ?)
            ''', (table_name, description, str(file_path), record_count))
            conn.commit()
        finally:
            conn.close()
        
        return jsonify({'success': True, 'message': '文件上传成功'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/check_existing_library', methods=['GET'])
def check_existing_library():
    try:
        exists = db_manager.check_existing_library()
        return jsonify({'exists': exists})
    except Exception as e:
        return jsonify({'exists': False, 'error': str(e)})

@app.route('/api/get_meddra_versions', methods=['GET'])
def get_meddra_versions():
    """获取MedDRA版本列表"""
    try:
        # 从数据库中获取MedDRA表信息
        tables = db_manager.get_database_tables()
        meddra_versions = []
        
        for table in tables.get('meddra', []):
            # 从表名或描述中提取版本信息
            version = table.get('name', '').replace('meddra_', '').replace('_', '.')
            if version:
                meddra_versions.append({
                    'version': version,
                    'name': table.get('name', ''),
                    'description': table.get('description', ''),
                    'record_count': table.get('record_count', 0)
                })
        
        return jsonify({'versions': meddra_versions})
    except Exception as e:
        return jsonify({'versions': [], 'error': str(e)})

@app.route('/api/get_whodrug_versions', methods=['GET'])
def get_whodrug_versions():
    """获取WHODrug版本列表"""
    try:
        # 从数据库中获取WHODrug表信息
        tables = db_manager.get_database_tables()
        whodrug_versions = []
        
        for table in tables.get('whodrug', []):
            # 从表名或描述中提取版本信息
            version = table.get('name', '').replace('whodrug_', '').replace('_', '.')
            if version:
                whodrug_versions.append({
                    'version': version,
                    'name': table.get('name', ''),
                    'description': table.get('description', ''),
                    'record_count': table.get('record_count', 0)
                })
        
        return jsonify({'versions': whodrug_versions})
    except Exception as e:
        return jsonify({'versions': [], 'error': str(e)})

@app.route('/api/get_ig_versions', methods=['GET'])
def get_ig_versions():
    """获取IG版本列表"""
    try:
        # 这里可以返回预定义的IG版本列表，或从配置文件中读取
        ig_versions = [
            {'version': '1.0', 'description': 'IG Version 1.0'},
            {'version': '1.1', 'description': 'IG Version 1.1'},
            {'version': '1.2', 'description': 'IG Version 1.2'},
            {'version': '2.0', 'description': 'IG Version 2.0'}
        ]
        
        return jsonify({'versions': ig_versions})
    except Exception as e:
        return jsonify({'versions': [], 'error': str(e)})

@app.route('/api/save_translation_library_config', methods=['POST'])
def save_translation_library_config():
    """保存翻译库配置"""
    try:
        data = request.get_json()
        
        # 验证必要字段
        required_fields = ['path', 'mode', 'translation_direction']
        for field in required_fields:
            if field not in data:
                return jsonify({'success': False, 'message': f'缺少必要字段: {field}'}), 400
        
        # 保存配置
        success = db_manager.save_translation_library_config(
            path=data['path'],
            mode=data['mode'],
            translation_direction=data['translation_direction'],
            meddra_version=data.get('meddra_version'),
            whodrug_version=data.get('whodrug_version'),
            ig_version=data.get('ig_version'),
            meddra_config=data.get('meddra_config'),
            whodrug_config=data.get('whodrug_config')
        )
        
        if success:
            return jsonify({'success': True, 'message': '配置保存成功'})
        else:
            return jsonify({'success': False, 'message': '配置保存失败'}), 500
            
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/load_translation_library_config', methods=['GET'])
def load_translation_library_config():
    """加载翻译库配置"""
    try:
        path = request.args.get('path')
        if not path:
            return jsonify({'success': False, 'message': '缺少path参数'}), 400
        
        config = db_manager.get_translation_library_config(path)
        
        if config:
            return jsonify({'success': True, 'config': config})
        else:
            return jsonify({'success': False, 'message': '未找到配置'}), 404
            
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/get_dataset_variables', methods=['GET'])
def get_dataset_variables():
    """获取数据集的所有变量名"""
    try:
        path = request.args.get('path')
        if not path:
            return jsonify({'error': '缺少path参数'}), 400
        
        if not os.path.exists(path):
            return jsonify({'error': '文件不存在'}), 404
        
        # 读取数据集并获取变量名
        if path.endswith('.csv'):
            df = pd.read_csv(path, nrows=0)  # 只读取列名
        elif path.endswith(('.xls', '.xlsx')):
            df = pd.read_excel(path, nrows=0)  # 只读取列名
        elif path.endswith('.sas7bdat'):
            df = pd.read_sas(path, chunksize=1).__next__().iloc[:0]  # 只读取列名
        else:
            return jsonify({'error': '不支持的文件格式'}), 400
        
        variables = df.columns.tolist()
        return jsonify({'variables': variables})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/get_dataset_variables', methods=['GET'])
def get_dataset_variables_by_name():
    """通过数据集名称获取变量列表"""
    try:
        dataset_name = request.args.get('dataset_name')
        file_path = request.args.get('file_path')
        
        print(f"[DEBUG] get_dataset_variables_by_name called with dataset_name={dataset_name}, file_path={file_path}")
        print(f"[DEBUG] processor.datasets keys: {list(processor.datasets.keys()) if processor.datasets else 'None'}")
        
        if dataset_name:
            # 通过数据集名称获取
            if dataset_name not in processor.datasets:
                print(f"[ERROR] 数据集 {dataset_name} 不存在于 processor.datasets 中")
                return jsonify({'success': False, 'error': f'数据集 {dataset_name} 不存在'}), 400
            
            dataset_info = processor.datasets[dataset_name]
            if processor.hide_supp_in_preview:
                df = dataset_info.get('data_view', dataset_info['data'])
            else:
                df = dataset_info['data']
                if dataset_name.upper().startswith('SUPP') and dataset_info.get('use_pivot_preview') and 'pivot_for_display' in dataset_info:
                    df = dataset_info['pivot_for_display']
            
            variables = list(df.columns)
            return jsonify({'success': True, 'variables': variables})
            
        elif file_path:
            # 通过文件路径获取（兼容旧接口）
            if not os.path.exists(file_path):
                return jsonify({'success': False, 'error': '文件不存在'})
            
            # 读取数据集并获取变量名
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path, nrows=0)
            elif file_path.endswith(('.xls', '.xlsx')):
                df = pd.read_excel(file_path, nrows=0)
            elif file_path.endswith('.sas7bdat'):
                df = pd.read_sas(file_path, chunksize=1).__next__().iloc[:0]
            else:
                return jsonify({'success': False, 'error': '不支持的文件格式'})
            
            variables = df.columns.tolist()
            return jsonify({'success': True, 'variables': variables})
        else:
            return jsonify({'success': False, 'error': '缺少dataset_name或file_path参数'})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

def should_translate_value(value, translation_direction):
    """判断值是否需要翻译"""
    if not value or pd.isna(value):
        return False
    
    value_str = str(value).strip()
    if not value_str:
        return False
    
    import re
    
    # 中译英：如果只包含英文字符、数字、空格和常见符号，则不需要翻译
    if translation_direction == 'zh_to_en':
        # 只包含ASCII字符（英文、数字、标点符号）
        if re.match(r'^[\x00-\x7F]+$', value_str):
            return False
    
    # 英译中：如果只包含中文字符、数字、空格和常见符号，则不需要翻译
    elif translation_direction == 'en_to_zh':
        # 只包含中文字符、数字、标点符号和空格
        if re.match(r'^[\u4e00-\u9fff\d\s，。！？；：""（）\[\]]+$', value_str):
            return False
    
    return True

def is_ai_translation_eligible(value):
    """判断值是否符合AI翻译条件"""
    if not value or pd.isna(value):
        return False
    
    value_str = str(value).strip()
    # 条件：长度在3-50字符之间，包含字母，不全是数字或特殊字符
    if (3 <= len(value_str) <= 50 and 
        any(c.isalpha() for c in value_str) and 
        not value_str.replace(' ', '').replace('-', '').replace('.', '').replace('/', '').isdigit()):
        return True
    return False

@app.route('/api/generate_coded_list', methods=['POST'])
def generate_coded_list():
    """生成编码清单"""
    try:
        data = request.get_json()
        
        # 验证必要字段
        if not data.get('translation_direction') or not data.get('path'):
            return jsonify({'success': False, 'message': '缺少必要的配置信息'}), 400
        
        path = data.get('path')
        translation_direction = data.get('translation_direction')
        
        # 初始化进度跟踪
        progress_info = {
            'total_datasets': 0,
            'processed_datasets': 0,
            'total_items': 0,
            'processed_items': 0,
            'current_stage': '获取配置信息',
            'current_dataset': '',
            'db_matched': 0,
            'ai_processing': 0,
            'ai_completed': 0
        }
        
        # 获取翻译库配置
        db_manager = DatabaseManager()
        config = db_manager.get_translation_library_config(path)
        
        if not config:
            return jsonify({'success': False, 'message': '未找到翻译库配置'}), 400
        
        meddra_config = config.get('meddra_config', [])
        whodrug_config = config.get('whodrug_config', [])
        meddra_version = config.get('meddra_version')
        whodrug_version = config.get('whodrug_version')
        
        # 更新进度：计算总数据集数量
        progress_info['total_datasets'] = len(meddra_config) + len(whodrug_config)
        progress_info['current_stage'] = '加载数据集'
        
        # 处理版本格式，从"27.1.english"格式提取"27.1"
        if meddra_version and '.' in meddra_version:
            # 提取版本号部分，去掉语言后缀
            version_parts = meddra_version.split('.')
            if len(version_parts) >= 2:
                meddra_version = f"{version_parts[0]}.{version_parts[1]}"
        
        if whodrug_version:
            # WHODrug版本格式处理：从"global.2025.mar.1.english"提取"2025 Mar 1"
            if 'global.' in whodrug_version:
                parts = whodrug_version.replace('global.', '').split('.')
                if len(parts) >= 3:
                    year = parts[0]
                    month = parts[1].capitalize()
                    day = parts[2]
                    whodrug_version = f"{year} {month} {day}"
        
        print(f'处理后的版本信息 - MedDRA: {meddra_version}, WHODrug: {whodrug_version}')
        
        # 使用全局processor实例获取合并后的数据
        # 检查是否已经加载了相同路径的数据
        if not processor.datasets or getattr(processor, 'current_path', None) != path:
            print(f'开始读取SAS文件: {path}')
            success, message = processor.read_sas_files(path, mode='SDTM')
            if not success:
                return jsonify({'success': False, 'message': f'读取SAS文件失败: {message}'}), 500
            processor.current_path = path  # 记录当前路径
            print(f'SAS文件读取完成，共加载 {len(processor.datasets)} 个数据集')
        
        coded_items = []
        # 统一收集所有未匹配的项目，用于批量AI翻译
        unmatched_items = {
            'meddra': [],  # MedDRA未匹配项目
            'whodrug': []  # WHODrug未匹配项目
        }
        
        print(f'开始处理编码清单，MedDRA配置项: {len(meddra_config)}, WHODrug配置项: {len(whodrug_config)}')
        
        # 批量处理MedDRA配置的变量 - 移除循环，使用pandas向量化操作
        if meddra_config:
            print(f'开始批量处理 {len(meddra_config)} 个MedDRA配置项')
            
            # 使用pandas DataFrame批量处理配置
            import pandas as pd
            meddra_df = pd.DataFrame(meddra_config)
            meddra_df['translation_direction'] = config.get('translation_direction', 'zh_to_en')
            
            # 批量验证数据集存在性
            meddra_df['dataset_exists'] = meddra_df['table_path'].isin(processor.datasets.keys())
            valid_meddra_configs = meddra_df[meddra_df['dataset_exists']].copy()
            
            if len(valid_meddra_configs) == 0:
                print('  ❌ 没有有效的MedDRA配置项')
            else:
                print(f'  ✅ 找到 {len(valid_meddra_configs)} 个有效的MedDRA配置项')
                
                # 预加载所有需要的数据集
                unique_datasets = valid_meddra_configs['table_path'].unique()
                dataset_cache = {}
                for ds_name in unique_datasets:
                    if ds_name in processor.datasets:
                        dataset_info = processor.datasets[ds_name]
                        dataset_cache[ds_name] = dataset_info.get('data_view', dataset_info['data'])
                
                # 批量收集所有唯一值和代码映射
                all_unique_values = set()
                all_code_values = set()
                config_value_mappings = []
                
                # 使用向量化操作处理每个配置
                for _, config_row in valid_meddra_configs.iterrows():
                    dataset_name = config_row['table_path']
                    variable_name = config_row['name_column']
                    code_variable = config_row['code_column']
                    
                    if dataset_name not in dataset_cache:
                        continue
                        
                    df = dataset_cache[dataset_name]
                    
                    if variable_name not in df.columns:
                        print(f'  ❌ 变量 {variable_name} 在数据集 {dataset_name} 中不存在，跳过')
                        continue
                    
                    # 获取唯一值（限制数量）
                    unique_values = df[variable_name].dropna().unique()[:10000]
                    
                    if len(unique_values) == 0:
                        continue
                    
                    # 构建值-代码映射
                    value_code_map = {}
                    if code_variable and code_variable in df.columns:
                        # 使用pandas向量化操作构建映射
                        temp_df = df[[variable_name, code_variable]].drop_duplicates()
                        temp_df = temp_df[temp_df[variable_name].isin(unique_values)]
                        
                        # 处理代码值格式
                        temp_df[code_variable] = temp_df[code_variable].apply(
                            lambda x: str(int(x)) if isinstance(x, float) and pd.notna(x) and x == int(x) else str(x) if pd.notna(x) else None
                        )
                        
                        value_code_map = dict(zip(temp_df[variable_name].astype(str), temp_df[code_variable]))
                        all_code_values.update(value_code_map.values())
                    
                    all_unique_values.update([str(v) for v in unique_values])
                    
                    config_value_mappings.append({
                        'dataset_name': dataset_name,
                        'variable_name': variable_name,
                        'code_variable': code_variable,
                        'unique_values': [str(v) for v in unique_values],
                        'value_code_map': value_code_map
                    })
                
                print(f'  📊 收集到 {len(all_unique_values)} 个唯一值，{len(all_code_values)} 个代码值')
                
                # 批量数据库查询
                translation_dict = {}
                try:
                    conn = sqlite3.connect('translation_db.sqlite')
                    
                    # 检查表存在性
                    cursor = conn.cursor()
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='meddra_merged'")
                    if not cursor.fetchone():
                        print('    ❌ meddra_merged表不存在')
                    else:
                        # 批量查询基于代码的翻译
                        if all_code_values:
                            code_list = [code for code in all_code_values if code]
                            if code_list:
                                placeholders = ','.join(['?'] * len(code_list))
                                query = f"SELECT code, name_cn, name_en FROM meddra_merged WHERE code IN ({placeholders}) AND version = ?"
                                
                                meddra_code_df = pd.read_sql_query(query, conn, params=code_list + [meddra_version])
                                print(f'    📊 代码查询结果: {len(meddra_code_df)} 条')
                                
                                # 构建代码到翻译的映射
                                translation_direction = config.get('translation_direction', 'zh_to_en')
                                if translation_direction == 'zh_to_en':
                                    code_trans_map = dict(zip(meddra_code_df['code'], meddra_code_df['name_en']))
                                else:
                                    code_trans_map = dict(zip(meddra_code_df['code'], meddra_code_df['name_cn']))
                                
                                # 将代码翻译映射到值翻译
                                for mapping in config_value_mappings:
                                    for value, code in mapping['value_code_map'].items():
                                        if code in code_trans_map:
                                            translation_dict[value] = code_trans_map[code]
                        
                        # 批量查询基于名称的翻译
                        if all_unique_values:
                            value_list = list(all_unique_values)
                            placeholders = ','.join(['?'] * len(value_list))
                            
                            translation_direction = config.get('translation_direction', 'zh_to_en')
                            if translation_direction == 'zh_to_en':
                                query = f"SELECT name_cn, name_en FROM meddra_merged WHERE name_cn IN ({placeholders}) AND version = ?"
                                meddra_name_df = pd.read_sql_query(query, conn, params=value_list + [meddra_version])
                                name_trans_map = dict(zip(meddra_name_df['name_cn'], meddra_name_df['name_en']))
                            else:
                                query = f"SELECT name_en, name_cn FROM meddra_merged WHERE name_en IN ({placeholders}) AND version = ?"
                                meddra_name_df = pd.read_sql_query(query, conn, params=value_list + [meddra_version])
                                name_trans_map = dict(zip(meddra_name_df['name_en'], meddra_name_df['name_cn']))
                            
                            print(f'    📊 名称查询结果: {len(name_trans_map)} 条')
                            
                            # 合并名称翻译（优先使用代码翻译）
                            for value, trans in name_trans_map.items():
                                if value not in translation_dict:
                                    translation_dict[value] = trans
                    
                    conn.close()
                    
                except Exception as e:
                    print(f'    ❌ 批量数据库查询错误: {e}')
                    translation_dict = {}
                
                print(f'    📝 获得翻译映射: {len(translation_dict)} 条')
                
                # 批量处理翻译结果
                matched_items = []
                unmatched_items_list = []
                
                # 使用列表推导式批量生成结果
                for mapping in config_value_mappings:
                    dataset_name = mapping['dataset_name']
                    variable_name = mapping['variable_name']
                    
                    # 更新进度
                    progress_info['current_dataset'] = f'{dataset_name}.{variable_name}'
                    
                    for value in mapping['unique_values']:
                        # 检查是否需要翻译
                        if not should_translate_value(value, config.get('translation_direction', 'zh_to_en')):
                            continue
                        
                        if value in translation_dict:
                            # 匹配项
                            matched_items.append({
                                'dataset': dataset_name,
                                'variable': variable_name,
                                'value': value,
                                'translated_value': translation_dict[value],
                                'translation_source': f'MedDRA {meddra_version}',
                                'translation_method': 'database',
                                'dictionary_type': 'meddra',
                                'dictionary_version': meddra_version,
                                'needs_confirmation': 'N'
                            })
                        else:
                            # 未匹配项
                            unmatched_items_list.append({
                                'index': len(coded_items) + len(matched_items) + len(unmatched_items_list),
                                'dataset': dataset_name,
                                'variable': variable_name,
                                'value': value,
                                'dictionary_type': 'meddra',
                                'dictionary_version': meddra_version
                            })
                
                # 批量添加到结果列表
                coded_items.extend(matched_items)
                unmatched_items['meddra'].extend(unmatched_items_list)
                
                # 批量添加未匹配项的占位符
                placeholder_items = [
                    {
                        'dataset': item['dataset'],
                        'variable': item['variable'],
                        'value': item['value'],
                        'translated_value': '',
                        'translation_source': '未翻译',
                        'translation_method': 'ai_pending',
                        'dictionary_type': 'meddra',
                        'dictionary_version': meddra_version,
                        'needs_confirmation': 1
                    }
                    for item in unmatched_items_list
                ]
                coded_items.extend(placeholder_items)
                
                # 批量保存未翻译的值到数据库
                if placeholder_items:
                    print(f'    💾 批量保存 {len(placeholder_items)} 个未翻译的MedDRA值')
                    try:
                        for item in placeholder_items:
                            db_manager.save_translation_result(
                                path=path,
                                translation_direction=config.get('translation_direction', 'zh_to_en'),
                                translation_type='编码清单',
                                dataset_name=item['dataset'],
                                variable_name=item['variable'],
                                original_value=item['value'],
                                translated_value='',
                                translation_source='未翻译',
                                needs_confirmation=True,
                                confidence_score=0.0
                            )
                    except Exception as e:
                        print(f'    ❌ 批量保存未翻译值失败: {e}')
                
                # 批量保存翻译结果
                if matched_items:
                    print(f'    💾 批量保存 {len(matched_items)} 个MedDRA翻译结果')
                    try:
                        # 使用批量操作保存翻译结果
                        for item in matched_items:
                            db_manager.save_translation_result(
                                path=path,
                                translation_direction=config.get('translation_direction', 'zh_to_en'),
                                translation_type='编码清单',
                                dataset_name=item['dataset'],
                                variable_name=item['variable'],
                                original_value=item['value'],
                                translated_value=item['translated_value'],
                                translation_source=item['translation_source'],
                                needs_confirmation=False,
                                confidence_score=1.0
                            )
                    except Exception as e:
                        print(f'    ❌ 批量保存翻译结果失败: {e}')
                
                print(f'  ✅ MedDRA批量处理完成: 数据库匹配 {len(matched_items)} 项，AI翻译队列 {len(unmatched_items_list)} 项')
                
                # 更新已处理数据集计数
                progress_info['processed_datasets'] += len(valid_meddra_configs)
        else:
            print('  ⚠️ 没有MedDRA配置项需要处理')
        
        # 批量处理WHODrug配置的变量 - 移除循环，使用pandas向量化操作
        if whodrug_config:
            print(f'开始批量处理 {len(whodrug_config)} 个WHODrug配置项')
            
            # 使用pandas DataFrame批量处理配置
            whodrug_df = pd.DataFrame(whodrug_config)
            whodrug_df['translation_direction'] = config.get('translation_direction', 'zh_to_en')
            
            # 批量验证数据集存在性
            whodrug_df['dataset_exists'] = whodrug_df['table_path'].isin(processor.datasets.keys())
            valid_whodrug_configs = whodrug_df[whodrug_df['dataset_exists']].copy()
            
            if len(valid_whodrug_configs) == 0:
                print('  ❌ 没有有效的WHODrug配置项')
            else:
                print(f'  ✅ 找到 {len(valid_whodrug_configs)} 个有效的WHODrug配置项')
                
                # 预加载所有需要的数据集
                unique_datasets = valid_whodrug_configs['table_path'].unique()
                dataset_cache = {}
                for ds_name in unique_datasets:
                    if ds_name in processor.datasets:
                        dataset_info = processor.datasets[ds_name]
                        dataset_cache[ds_name] = dataset_info.get('data_view', dataset_info['data'])
                
                # 批量收集所有唯一值和代码映射
                all_unique_values = set()
                all_code_values = set()
                config_value_mappings = []
                
                # 使用向量化操作处理每个配置
                for _, config_row in valid_whodrug_configs.iterrows():
                    dataset_name = config_row['table_path']
                    variable_name = config_row['name_column']
                    code_variable = config_row['code_column']
                    
                    if dataset_name not in dataset_cache:
                        continue
                        
                    df = dataset_cache[dataset_name]
                    
                    if variable_name not in df.columns:
                        print(f'  ❌ 变量 {variable_name} 在数据集 {dataset_name} 中不存在，跳过')
                        continue
                    
                    # 获取唯一值（限制数量）
                    unique_values = df[variable_name].dropna().unique()[:10000]
                    
                    if len(unique_values) == 0:
                        continue
                    
                    # 构建值-代码映射
                    value_code_map = {}
                    if code_variable and code_variable in df.columns:
                        # 使用pandas向量化操作构建映射
                        temp_df = df[[variable_name, code_variable]].drop_duplicates()
                        temp_df = temp_df[temp_df[variable_name].isin(unique_values)]
                        
                        # 处理代码值格式
                        temp_df[code_variable] = temp_df[code_variable].apply(
                            lambda x: str(int(x)) if isinstance(x, float) and pd.notna(x) and x == int(x) else str(x) if pd.notna(x) else None
                        )
                        
                        value_code_map = dict(zip(temp_df[variable_name].astype(str), temp_df[code_variable]))
                        all_code_values.update(value_code_map.values())
                    
                    all_unique_values.update([str(v) for v in unique_values])
                    
                    config_value_mappings.append({
                        'dataset_name': dataset_name,
                        'variable_name': variable_name,
                        'code_variable': code_variable,
                        'unique_values': [str(v) for v in unique_values],
                        'value_code_map': value_code_map
                    })
                
                print(f'  📊 收集到 {len(all_unique_values)} 个唯一值，{len(all_code_values)} 个代码值')
                
                # 批量数据库查询
                translation_dict = {}
                try:
                    conn = sqlite3.connect('translation_db.sqlite')
                    
                    # 检查表存在性
                    cursor = conn.cursor()
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='whodrug_merged'")
                    if not cursor.fetchone():
                        print('    ❌ whodrug_merged表不存在')
                    else:
                        # 批量查询基于代码的翻译
                        if all_code_values:
                            code_list = [code for code in all_code_values if code]
                            if code_list:
                                placeholders = ','.join(['?'] * len(code_list))
                                query = f"SELECT code, name_cn, name_en FROM whodrug_merged WHERE code IN ({placeholders}) AND version = ?"
                                
                                whodrug_code_df = pd.read_sql_query(query, conn, params=code_list + [whodrug_version])
                                print(f'    📊 代码查询结果: {len(whodrug_code_df)} 条')
                                
                                # 构建代码到翻译的映射
                                translation_direction = config.get('translation_direction', 'zh_to_en')
                                if translation_direction == 'zh_to_en':
                                    code_trans_map = dict(zip(whodrug_code_df['code'], whodrug_code_df['name_en']))
                                else:
                                    code_trans_map = dict(zip(whodrug_code_df['code'], whodrug_code_df['name_cn']))
                                
                                # 将代码翻译映射到值翻译
                                for mapping in config_value_mappings:
                                    for value, code in mapping['value_code_map'].items():
                                        if code in code_trans_map:
                                            translation_dict[value] = code_trans_map[code]
                        
                        # 批量查询基于名称的翻译
                        if all_unique_values:
                            value_list = list(all_unique_values)
                            placeholders = ','.join(['?'] * len(value_list))
                            
                            translation_direction = config.get('translation_direction', 'zh_to_en')
                            if translation_direction == 'zh_to_en':
                                query = f"SELECT name_cn, name_en FROM whodrug_merged WHERE name_cn IN ({placeholders}) AND version = ?"
                                whodrug_name_df = pd.read_sql_query(query, conn, params=value_list + [whodrug_version])
                                name_trans_map = dict(zip(whodrug_name_df['name_cn'], whodrug_name_df['name_en']))
                            else:
                                query = f"SELECT name_en, name_cn FROM whodrug_merged WHERE name_en IN ({placeholders}) AND version = ?"
                                whodrug_name_df = pd.read_sql_query(query, conn, params=value_list + [whodrug_version])
                                name_trans_map = dict(zip(whodrug_name_df['name_en'], whodrug_name_df['name_cn']))
                            
                            print(f'    📊 名称查询结果: {len(name_trans_map)} 条')
                            
                            # 合并名称翻译（优先使用代码翻译）
                            for value, trans in name_trans_map.items():
                                if value not in translation_dict:
                                    translation_dict[value] = trans
                    
                    conn.close()
                    
                except Exception as e:
                    print(f'    ❌ 批量数据库查询错误: {e}')
                    translation_dict = {}
                
                print(f'    📝 获得翻译映射: {len(translation_dict)} 条')
                
                # 批量处理翻译结果
                matched_items = []
                unmatched_items_list = []
                
                # 使用列表推导式批量生成结果
                for mapping in config_value_mappings:
                    dataset_name = mapping['dataset_name']
                    variable_name = mapping['variable_name']
                    
                    # 更新进度
                    progress_info['current_dataset'] = f'{dataset_name}.{variable_name}'
                    
                    for value in mapping['unique_values']:
                        # 检查是否需要翻译
                        if not should_translate_value(value, config.get('translation_direction', 'zh_to_en')):
                            continue
                        
                        if value in translation_dict:
                            # 匹配项
                            matched_items.append({
                                'dataset': dataset_name,
                                'variable': variable_name,
                                'value': value,
                                'translated_value': translation_dict[value],
                                'translation_source': f'WHODrug {whodrug_version}',
                                'translation_method': 'database',
                                'dictionary_type': 'whodrug',
                                'dictionary_version': whodrug_version,
                                'needs_confirmation': False
                            })
                        else:
                            # 未匹配项
                            unmatched_items_list.append({
                                'index': len(coded_items) + len(matched_items) + len(unmatched_items_list),
                                'dataset': dataset_name,
                                'variable': variable_name,
                                'value': value,
                                'dictionary_type': 'whodrug',
                                'dictionary_version': whodrug_version
                            })
                
                # 批量添加到结果列表
                coded_items.extend(matched_items)
                unmatched_items['whodrug'].extend(unmatched_items_list)
                
                # 批量添加未匹配项的占位符
                placeholder_items = [
                    {
                        'dataset': item['dataset'],
                        'variable': item['variable'],
                        'value': item['value'],
                        'translated_value': '',
                        'translation_source': '未翻译',
                        'translation_method': 'ai_pending',
                        'dictionary_type': 'whodrug',
                        'dictionary_version': whodrug_version,
                        'needs_confirmation': True
                    }
                    for item in unmatched_items_list
                ]
                coded_items.extend(placeholder_items)
                
                # 批量保存未翻译的值到数据库
                if placeholder_items:
                    print(f'    💾 批量保存 {len(placeholder_items)} 个未翻译的WHODrug值')
                    try:
                        for item in placeholder_items:
                            db_manager.save_translation_result(
                                path=path,
                                translation_direction=config.get('translation_direction', 'zh_to_en'),
                                translation_type='编码清单',
                                dataset_name=item['dataset'],
                                variable_name=item['variable'],
                                original_value=item['value'],
                                translated_value='',
                                translation_source='未翻译',
                                needs_confirmation=True,
                                confidence_score=0.0
                            )
                    except Exception as e:
                        print(f'    ❌ 批量保存未翻译值失败: {e}')
                
                # 批量保存翻译结果
                if matched_items:
                    print(f'    💾 批量保存 {len(matched_items)} 个WHODrug翻译结果')
                    try:
                        # 使用批量操作保存翻译结果
                        for item in matched_items:
                            db_manager.save_translation_result(
                                path=path,
                                translation_direction=config.get('translation_direction', 'zh_to_en'),
                                translation_type='编码清单',
                                dataset_name=item['dataset'],
                                variable_name=item['variable'],
                                original_value=item['value'],
                                translated_value=item['translated_value'],
                                translation_source=item['translation_source'],
                                needs_confirmation=False,
                                confidence_score=1.0
                            )
                    except Exception as e:
                        print(f'    ❌ 批量保存翻译结果失败: {e}')
                
                print(f'  ✅ WHODrug批量处理完成: 数据库匹配 {len(matched_items)} 项，AI翻译队列 {len(unmatched_items_list)} 项')
                
                # 更新已处理数据集计数
                progress_info['processed_datasets'] += len(valid_whodrug_configs)
        else:
            print('  ⚠️ 没有WHODrug配置项需要处理')
        
        # 移除原有的单独循环处理逻辑，统一使用上面的批量处理
        # for idx, meddra_item in enumerate(meddra_config):
        # 上面的批量处理已经完成了所有MedDRA和WHODrug配置的处理
        # 移除原有的单独循环处理逻辑
        
        # 原有的WHODrug循环处理逻辑已被上面的批量处理替代
        # WHODrug批量处理已完成
        # 所有循环处理逻辑已被批量处理替代

                        
        
        # 使用pandas批量连接数据库，优化数据连接性能
        print(f'\n开始批量数据库连接，MedDRA未匹配: {len(unmatched_items["meddra"])}项，WHODrug未匹配: {len(unmatched_items["whodrug"])}项')
        
        # 更新进度：开始数据库连接阶段
        progress_info['current_stage'] = '批量数据库连接'
        
        # 处理MedDRA未匹配项目 - 使用pandas left join
        if unmatched_items['meddra']:
            print(f'批量连接MedDRA数据库: {len(unmatched_items["meddra"])}项')
            progress_info['current_dataset'] = 'MedDRA 数据库连接'
            
            try:
                import pandas as pd
                
                # 构建待查询的DataFrame
                meddra_df = pd.DataFrame([
                    {
                        'index': item['index'],
                        'dataset': item['dataset'],
                        'variable': item['variable'],
                        'value': item['value'],
                        'dictionary_type': item['dictionary_type'],
                        'dictionary_version': item['dictionary_version']
                    } for item in unmatched_items['meddra']
                ])
                
                # 从数据库读取MedDRA翻译表
                conn = sqlite3.connect('translation_db.sqlite')
                if translation_direction == 'zh_to_en':
                    db_query = f"SELECT name_cn as source_value, name_en as target_value FROM meddra_merged WHERE version = '{meddra_version}'"
                else:
                    db_query = f"SELECT name_en as source_value, name_cn as target_value FROM meddra_merged WHERE version = '{meddra_version}'"
                
                meddra_trans_df = pd.read_sql_query(db_query, conn)
                conn.close()
                
                print(f'    从数据库读取到 {len(meddra_trans_df)} 条MedDRA翻译记录')
                
                # 使用pandas left join进行批量连接
                merged_df = meddra_df.merge(
                    meddra_trans_df, 
                    left_on='value', 
                    right_on='source_value', 
                    how='left'
                )
                
                # 更新coded_items
                for _, row in merged_df.iterrows():
                    idx = row['index']
                    if idx < len(coded_items):
                        if pd.notna(row['target_value']):
                            # 数据库中找到翻译
                            coded_items[idx]['translated_value'] = row['target_value']
                            coded_items[idx]['translation_source'] = f'MedDRA {meddra_version}'
                            coded_items[idx]['translation_method'] = 'database'
                            coded_items[idx]['needs_confirmation'] = 0
                            
                            # 保存翻译结果到数据库
                            db_manager.save_translation_result(
                                path=path,
                                translation_direction=translation_direction,
                                translation_type='编码清单',
                                dataset_name=row['dataset'],
                                variable_name=row['variable'],
                                original_value=row['value'],
                                translated_value=row['target_value'],
                                translation_source=f'MedDRA {meddra_version}',
                                needs_confirmation=False,
                                confidence_score=1.0
                            )
                        else:
                            # 数据库中未找到翻译，输出空值
                            coded_items[idx]['translated_value'] = ''
                            coded_items[idx]['translation_source'] = '未翻译'
                            coded_items[idx]['translation_method'] = 'database'
                            coded_items[idx]['needs_confirmation'] = True
                
                matched_count = len(merged_df[pd.notna(merged_df['target_value'])])
                print(f'MedDRA数据库连接完成，匹配到 {matched_count}/{len(unmatched_items["meddra"])} 项')
                
            except Exception as e:
                print(f'MedDRA批量数据库连接失败: {e}')
                # 异常时输出空值
                for item in unmatched_items['meddra']:
                    if item['index'] < len(coded_items):
                        coded_items[item['index']]['translated_value'] = ''
                        coded_items[item['index']]['translation_source'] = '未翻译'
                        coded_items[item['index']]['translation_method'] = 'database'
                        coded_items[item['index']]['needs_confirmation'] = True
        
        # 处理WHODrug未匹配项目 - 使用pandas left join
        if unmatched_items['whodrug']:
            print(f'批量连接WHODrug数据库: {len(unmatched_items["whodrug"])}项')
            progress_info['current_dataset'] = 'WHODrug 数据库连接'
            
            try:
                import pandas as pd
                
                # 构建待查询的DataFrame
                whodrug_df = pd.DataFrame([
                    {
                        'index': item['index'],
                        'dataset': item['dataset'],
                        'variable': item['variable'],
                        'value': item['value'],
                        'dictionary_type': item['dictionary_type'],
                        'dictionary_version': item['dictionary_version']
                    } for item in unmatched_items['whodrug']
                ])
                
                # 从数据库读取WHODrug翻译表
                conn = sqlite3.connect('translation_db.sqlite')
                if translation_direction == 'zh_to_en':
                    db_query = f"SELECT name_cn as source_value, name_en as target_value FROM whodrug_merged WHERE version = '{whodrug_version}'"
                else:
                    db_query = f"SELECT name_en as source_value, name_cn as target_value FROM whodrug_merged WHERE version = '{whodrug_version}'"
                
                whodrug_trans_df = pd.read_sql_query(db_query, conn)
                conn.close()
                
                print(f'    从数据库读取到 {len(whodrug_trans_df)} 条WHODrug翻译记录')
                
                # 使用pandas left join进行批量连接
                merged_df = whodrug_df.merge(
                    whodrug_trans_df, 
                    left_on='value', 
                    right_on='source_value', 
                    how='left'
                )
                
                # 更新coded_items
                for _, row in merged_df.iterrows():
                    idx = row['index']
                    if idx < len(coded_items):
                        if pd.notna(row['target_value']):
                            # 数据库中找到翻译
                            coded_items[idx]['translated_value'] = row['target_value']
                            coded_items[idx]['translation_source'] = f'WHODrug {whodrug_version}'
                            coded_items[idx]['translation_method'] = 'database'
                            coded_items[idx]['needs_confirmation'] = False
                            
                            # 保存翻译结果到数据库
                            db_manager.save_translation_result(
                                path=path,
                                translation_direction=translation_direction,
                                translation_type='编码清单',
                                dataset_name=row['dataset'],
                                variable_name=row['variable'],
                                original_value=row['value'],
                                translated_value=row['target_value'],
                                translation_source=f'WHODrug {whodrug_version}',
                                needs_confirmation=False,
                                confidence_score=1.0
                            )
                        else:
                            # 数据库中未找到翻译，输出空值
                            coded_items[idx]['translated_value'] = ''
                            coded_items[idx]['translation_source'] = '未翻译'
                            coded_items[idx]['translation_method'] = 'database'
                            coded_items[idx]['needs_confirmation'] = 1
                
                matched_count = len(merged_df[pd.notna(merged_df['target_value'])])
                print(f'WHODrug数据库连接完成，匹配到 {matched_count}/{len(unmatched_items["whodrug"])} 项')
                
            except Exception as e:
                print(f'WHODrug批量数据库连接失败: {e}')
                # 异常时输出空值
                for item in unmatched_items['whodrug']:
                    if item['index'] < len(coded_items):
                        coded_items[item['index']]['translated_value'] = ''
                        coded_items[item['index']]['translation_source'] = '未翻译'
                        coded_items[item['index']]['translation_method'] = 'database'
                        coded_items[item['index']]['needs_confirmation'] = True
        
        # 检测重复值并标记待确认
        # 创建一个字典来跟踪每个变量-值组合的翻译结果
        value_translations = {}
        for item in coded_items:
            key = f"{item['variable']}|{item['value']}"
            if key not in value_translations:
                value_translations[key] = set()
            if item['translated_value']:  # 只记录有翻译值的项
                value_translations[key].add(item['translated_value'])
        
        # 标记有多个翻译结果的项为待确认
        for item in coded_items:
            key = f"{item['variable']}|{item['value']}"
            if len(value_translations.get(key, set())) > 1:
                item['needs_confirmation'] = True
                item['highlight'] = True  # 添加高亮标记
            else:
                item['highlight'] = False
        
        # 按照用户要求排序：AI翻译在前，数据库翻译在后
        ai_items = [item for item in coded_items if item['translation_source'] in ['AI', 'AI_FAILED']]
        db_items = [item for item in coded_items if item['translation_source'] not in ['AI', 'AI_FAILED', '未翻译']]
        other_items = [item for item in coded_items if item['translation_source'] == '未翻译']
        
        # 重新排列：AI翻译在前，数据库翻译在后
        sorted_coded_items = ai_items + db_items + other_items
        
        # 限制返回的项目数量以提高响应速度
        max_items = 500  # 限制最多返回500个项目
        limited_coded_items = sorted_coded_items[:max_items] if len(sorted_coded_items) > max_items else sorted_coded_items
        
        # 最终进度更新
        progress_info['current_stage'] = '完成处理'
        progress_info['processed_items'] = len(sorted_coded_items)
        progress_info['ai_completed'] = progress_info['ai_processing']
        
        # 统计翻译来源
        db_matched = sum(1 for item in sorted_coded_items if item['translation_source'] not in ['AI', 'AI_FAILED', ''])
        ai_translated = sum(1 for item in sorted_coded_items if item['translation_source'] in ['AI', 'AI_FAILED'] and item['translated_value'])
        untranslated = sum(1 for item in sorted_coded_items if not item['translated_value'])
        
        # 更新进度统计
        progress_info['db_matched'] = db_matched
        progress_info['ai_completed'] = ai_translated
        
        result = {
            'success': True,
            'message': f'编码清单生成成功（显示前{len(limited_coded_items)}项，共{len(coded_items)}项）',
            'data': {
                'translation_direction': translation_direction,
                'meddra_version': meddra_version,
                'whodrug_version': whodrug_version,
                'coded_items': limited_coded_items,
                'total_count': len(sorted_coded_items),
                'displayed_count': len(limited_coded_items),
                'is_limited': len(sorted_coded_items) > max_items,
                'translation_stats': {
                    'db_matched': db_matched,
                    'ai_translated': ai_translated,
                    'untranslated': untranslated,
                    'total_meddra_unmatched': len(unmatched_items['meddra']),
                    'total_whodrug_unmatched': len(unmatched_items['whodrug'])
                },
                'progress_info': progress_info
            }
        }
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/generate_uncoded_list', methods=['POST'])
def generate_uncoded_list():
    """生成非编码清单"""
    try:
        data = request.get_json()
        
        # 验证必要字段
        if not data.get('translation_direction') or not data.get('path'):
            return jsonify({'success': False, 'message': '缺少翻译方向或项目路径配置'}), 400
        
        translation_direction = data.get('translation_direction')
        path = data.get('path')
        
        # 获取翻译库配置
        db_manager = DatabaseManager()
        translation_config = db_manager.get_translation_library_config(path)
        
        if not translation_config:
            return jsonify({'success': False, 'message': '未找到翻译库配置，请先保存配置'}), 400
        
        # 使用全局processor实例获取合并后的数据
        # 确保processor已经加载了数据
        if not processor.datasets or getattr(processor, 'current_path', None) != path:
            success, message = processor.read_sas_files(path, mode='SDTM')
            if not success:
                return jsonify({'success': False, 'message': f'读取SAS文件失败: {message}'}), 500
            processor.current_path = path
        
        # 获取MedDRA和WHODrug配置表中的变量名
        meddra_config = translation_config.get('meddra_config', [])
        whodrug_config = translation_config.get('whodrug_config', [])
        
        # 使用向量化操作收集编码变量名
        import pandas as pd
        coded_variables = set()
        
        # 从MedDRA配置中提取变量名
        if meddra_config:
            meddra_df = pd.DataFrame(meddra_config)
            coded_variables.update(meddra_df['name_column'].dropna().unique())
        
        # 从WHODrug配置中提取变量名
        if whodrug_config:
            whodrug_df = pd.DataFrame(whodrug_config)
            coded_variables.update(whodrug_df['name_column'].dropna().unique())
        
        print(f'编码变量列表: {coded_variables}')
        
        # 使用pandas批量处理所有数据集，收集非编码变量的唯一值
        all_uncoded_data = []
        
        for dataset_name, dataset_info in processor.datasets.items():
            df = dataset_info.get('data_view', dataset_info['data'])
            meta = dataset_info.get('meta')
            
            # 筛选出非编码变量
            uncoded_columns = [col for col in df.columns if col not in coded_variables]
            
            if not uncoded_columns:
                continue
            
            # 获取变量标签信息
            variable_labels = {}
            if meta and hasattr(meta, 'column_labels') and meta.column_labels:
                for i, column in enumerate(df.columns):
                    if i < len(meta.column_labels) and meta.column_labels[i]:
                        variable_labels[column] = meta.column_labels[i]
            
            # 使用pandas向量化操作处理每个非编码变量
            for column in uncoded_columns:
                # 获取该变量的非空唯一值
                unique_values = df[column].dropna().unique()
                
                if len(unique_values) == 0:
                    continue
                
                # 限制每个变量的唯一值数量，避免数据过多
                if len(unique_values) > 100:
                    unique_values = unique_values[:100]
                
                # 获取变量标签
                variable_label = variable_labels.get(column, '')
                
                # 批量创建记录
                for value in unique_values:
                    all_uncoded_data.append({
                        'dataset': dataset_name,
                        'variable': column,
                        'variable_label': variable_label,
                        'value': str(value)
                    })
        
        print(f'收集到 {len(all_uncoded_data)} 个非编码数据项')
        
        # 创建DataFrame进行去重处理
        if not all_uncoded_data:
            return jsonify({
                'success': True,
                'message': '未找到非编码数据项',
                'data': {
                    'translation_direction': translation_direction,
                    'uncoded_items': [],
                    'total_count': 0
                }
            })
        
        uncoded_df = pd.DataFrame(all_uncoded_data)
        
        # 在去重前过滤掉只包含数值、日期、单位的数据
        def is_numeric_date_or_unit_only(row):
            """结合变量标签判断值是否只包含数值、日期或单位"""
            import re
            value_str = str(row['value']).strip()
            variable_label = str(row.get('variable_label', '')).lower()
            variable_name = str(row.get('variable', '')).lower()
            
            # 空值或过短的值直接过滤
            if not value_str or len(value_str) <= 2:
                return True
            
            # 基于变量标签和变量名判断日期时间变量
            date_keywords = ['date', 'time', 'datetime', 'day', 'month', 'year', 
                           '日期', '时间', '年', '月', '日', 'dt', 'tm', 'visit']
            if any(keyword in variable_label or keyword in variable_name for keyword in date_keywords):
                # 对于日期时间变量，更严格地判断
                date_patterns = [
                    r'^\d{4}[-/]\d{1,2}[-/]\d{1,2}$',  # YYYY-MM-DD
                    r'^\d{1,2}[-/]\d{1,2}[-/]\d{4}$',  # MM-DD-YYYY
                    r'^\d{4}\d{2}\d{2}$',              # YYYYMMDD
                    r'^\d{2}\w{3}\d{4}$',              # DDMMMYYYY
                    r'^\d{1,2}:\d{2}(:\d{2})?$',      # HH:MM:SS
                ]
                for pattern in date_patterns:
                    if re.match(pattern, value_str, re.IGNORECASE):
                        return True
            
            # 基于变量标签和变量名判断单位变量
            unit_keywords = ['unit', 'weight', 'height', 'dose', 'volume', 'temp', 
                           '单位', '重量', '身高', '剂量', '体积', '温度', 'kg', 'cm', 'mg']
            if any(keyword in variable_label or keyword in variable_name for keyword in unit_keywords):
                # 对于单位变量，检查是否为纯单位值
                units = ['kg', 'g', 'mg', 'μg', 'ng', 'pg', 'l', 'ml', 'μl', 'dl',
                        'cm', 'mm', 'm', 'km', 'in', 'ft', 'h', 'min', 's', 'hr',
                        'bpm', 'mmhg', '°c', '°f', '%', 'percent']
                if value_str.lower() in units:
                    return True
                # 数字+单位组合
                if re.match(r'^[+-]?\d*\.?\d+\s*[a-zA-Z%°μ]+$', value_str):
                    return True
            
            # 纯数字（包括小数、负数、科学计数法）
            if re.match(r'^[+-]?\d*\.?\d+([eE][+-]?\d+)?$', value_str):
                return True
            
            # 简单分类值
            simple_values = ['yes', 'no', 'y', 'n', 'male', 'female', 'm', 'f',
                           'left', 'right', 'bilateral', 'na', 'null', 'missing']
            if value_str.lower() in simple_values:
                return True
            
            # 单个字符或数字
            if len(value_str) == 1 and (value_str.isdigit() or value_str.isalpha()):
                return True
            
            return False
        
        # 应用过滤函数
        initial_count = len(uncoded_df)
        uncoded_df = uncoded_df[~uncoded_df.apply(is_numeric_date_or_unit_only, axis=1)]
        filtered_count = len(uncoded_df)
        print(f'过滤数值/日期/单位数据：从 {initial_count} 项减少到 {filtered_count} 项')
        
        # 按数据集、变量名、变量值去重
        uncoded_df = uncoded_df.drop_duplicates(subset=['dataset', 'variable', 'value'])
        print(f'去重后剩余 {len(uncoded_df)} 个唯一数据项')
        
        # 添加翻译相关字段，设置翻译来源为"未翻译"
        uncoded_df['translated_value'] = ''
        uncoded_df['translation_source'] = '未翻译'
        uncoded_df['translation_method'] = 'uncoded'
        uncoded_df['needs_confirmation'] = 1
        uncoded_df['confidence_score'] = 0.0
        
        # 批量保存到translation_results数据库
        print('开始批量保存翻译结果到数据库')
        
        # 批量保存结果
        saved_count = 0
        for _, row in uncoded_df.iterrows():
            try:
                result = db_manager.save_translation_result(
                    path=path,
                    translation_direction=translation_direction,
                    translation_type='非编码清单',
                    dataset_name=row['dataset'],
                    variable_name=row['variable'],
                    original_value=row['value'],
                    translated_value=row['translated_value'],
                    translation_source=row['translation_source'],
                    needs_confirmation=True,
                    confidence_score=row['confidence_score']
                )
                if result:
                    saved_count += 1
            except Exception as e:
                print(f'保存单条记录时发生错误: {str(e)}')
                continue
        
        print(f'成功批量保存 {saved_count} 条翻译结果到数据库')
        
        # 转换为返回格式
        uncoded_items = uncoded_df[[
            'dataset', 'variable', 'value', 'translated_value', 
            'translation_source', 'needs_confirmation'
        ]].to_dict('records')
        
        # 按数据集名称和变量名排序
        uncoded_items.sort(key=lambda x: (x['dataset'], x['variable'], x['value']))
        
        # 限制返回数量以提高响应速度
        max_items = 500
        if len(uncoded_items) > max_items:
            uncoded_items = uncoded_items[:max_items]
        
        result = {
            'success': True,
            'message': '非编码清单生成成功',
            'data': {
                'translation_direction': translation_direction,
                'uncoded_items': uncoded_items,
                'total_count': len(uncoded_df),
                'returned_count': len(uncoded_items)
            }
        }
        
        return jsonify(result)
        
    except Exception as e:
        print(f'生成非编码清单时发生错误: {e}')
        return jsonify({'success': False, 'message': str(e)}), 500





@app.route('/api/generate_dataset_label', methods=['POST'])
def generate_dataset_label():
    """生成数据集Label"""
    try:
        data = request.get_json()
        
        # 验证必要字段
        if not data.get('translation_direction') or not data.get('path'):
            return jsonify({'success': False, 'message': '缺少翻译方向或项目路径配置'}), 400
        
        translation_direction = data.get('translation_direction')
        path = data.get('path')
        
        # 获取翻译库配置
        db_manager = DatabaseManager()
        translation_config = db_manager.get_translation_library_config(path)
        
        if not translation_config:
            return jsonify({'success': False, 'message': '未找到翻译库配置，请先保存配置'}), 400
        
        ig_version = translation_config.get('ig_version')
        if not ig_version:
            return jsonify({'success': False, 'message': '未找到IG版本配置'}), 400
        
        # 使用全局processor实例获取合并后的数据
        # 确保processor已经加载了数据
        if not processor.datasets:
            processor.read_sas_files(path)
        
        # 使用pandas批量处理数据集标签
        import pandas as pd
        import sqlite3
        
        # 1. 使用向量化操作收集所有数据集信息
        dataset_names = list(processor.datasets.keys())
        dataset_info_list = []
        
        # 批量处理数据集标签提取
        def extract_label(dataset_name):
            dataset_info = processor.datasets[dataset_name]
            meta = dataset_info.get('meta')
            
            if meta and hasattr(meta, 'table_label') and meta.table_label:
                return meta.table_label
            elif meta and hasattr(meta, 'file_label') and meta.file_label:
                return meta.file_label
            else:
                return dataset_name
        
        # 使用列表推导式批量处理
        dataset_info_list = [
            {'dataset': name, 'original_label': extract_label(name)}
            for name in dataset_names
        ]
        
        # 2. 创建DataFrame
        df_datasets = pd.DataFrame(dataset_info_list)
        
        # 3. 批量查询SDTM数据库（参考generate_variable_label的方式）
        dataset_translations = db_manager.get_batch_dataset_translations(dataset_names, ig_version)
        
        # 4. 创建翻译结果DataFrame
        translation_data = []
        for dataset_name in dataset_names:
            translation = dataset_translations.get(dataset_name)
            translated_label = ''
            translation_source = '未翻译'
            needs_confirmation = True
            
            if translation:
                if translation_direction == 'en_to_zh':
                    translated_label = translation.get('name_cn', '')
                else:
                    translated_label = translation.get('name_en', '')
                
                if translated_label:
                    translation_source = ig_version
                    needs_confirmation = False
            
            translation_data.append({
                'dataset': dataset_name,
                'translated_label': translated_label,
                'translation_source': translation_source,
                'needs_confirmation': needs_confirmation
            })
        
        df_translations = pd.DataFrame(translation_data)
        
        # 5. 使用pandas left join批量连接数据
        df_result = df_datasets.merge(
            df_translations, 
            on='dataset', 
            how='left'
        )
        
        # 转换needs_confirmation为字符串格式
        df_result['needs_confirmation'] = df_result['needs_confirmation'].apply(
            lambda x: 'Y' if x else 'N'
        )
        df_result['version'] = ig_version
        
        # 7. 批量保存翻译结果到数据库（仅保存有翻译的项目）
        translated_items = df_result[df_result['translated_label'] != '']
        if not translated_items.empty:
            # 使用向量化操作批量保存
            save_data = translated_items.apply(
                lambda row: db_manager.save_translation_result(
                    path, translation_direction, '数据集标签', row['dataset'], 
                    'LABEL', row['original_label'], row['translated_label'], 
                    row['translation_source'], row['needs_confirmation'] == 1
                ), axis=1
            )
        
        # 统计翻译结果
        db_matched_count = len(df_result[df_result['translated_label'] != ''])
        untranslated_count = len(df_result[df_result['translated_label'] == ''])
        total_count = len(df_result)
        
        # 8. 转换为最终结果格式
        dataset_labels = df_result[[
            'dataset', 'original_label', 'translated_label', 
            'translation_source', 'needs_confirmation', 'version'
        ]].to_dict('records')
        
        result = {
            'success': True,
            'message': '数据集Label生成成功',
            'data': {
                'translation_direction': translation_direction,
                'ig_version': ig_version,
                'dataset_labels': dataset_labels,
                'summary': {
                    'total': total_count,
                    'db_matched': db_matched_count,
                    'untranslated': untranslated_count
                }
            }
        }
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/generate_variable_label', methods=['POST'])
def generate_variable_label():
    """生成变量Label"""
    try:
        data = request.get_json()
        
        # 验证必要字段
        if not data.get('translation_direction') or not data.get('path'):
            return jsonify({'success': False, 'message': '缺少翻译方向或项目路径配置'}), 400
        
        translation_direction = data.get('translation_direction')
        path = data.get('path')
        
        # 获取翻译库配置
        db_manager = DatabaseManager()
        translation_config = db_manager.get_translation_library_config(path)
        
        if not translation_config:
            return jsonify({'success': False, 'message': '未找到翻译库配置，请先保存配置'}), 400
        
        ig_version = translation_config.get('ig_version')
        if not ig_version:
            return jsonify({'success': False, 'message': '未找到IG版本配置'}), 400
        
        # 使用全局processor实例获取合并后的数据
        # 确保processor已经加载了数据
        if not processor.datasets:
            processor.read_sas_files(path)
        
        # 生成变量标签清单
        variable_labels = []
        db_matched_count = 0
        untranslated_count = 0
        total_count = 0
        
        # 1. 使用向量化操作收集所有数据集和变量信息
        def extract_variable_info(dataset_name, dataset_info):
            df = dataset_info['data']
            meta = dataset_info.get('meta')
            
            # 使用向量化操作处理列信息
            columns = list(df.columns)
            original_labels = []
            
            # 批量处理标签提取
            if meta and hasattr(meta, 'column_labels') and meta.column_labels:
                for i, column in enumerate(columns):
                    try:
                        if i < len(meta.column_labels):
                            original_label = meta.column_labels[i] or column
                        else:
                            original_label = column
                    except (ValueError, IndexError):
                        original_label = column
                    original_labels.append(original_label)
            else:
                original_labels = columns
            
            # 使用列表推导式批量创建变量数据
            variables_data = [
                {
                    'dataset': dataset_name,
                    'variable': column,
                    'original_label': original_label
                }
                for column, original_label in zip(columns, original_labels)
            ]
            return variables_data
        
        # 使用列表推导式批量处理所有数据集
        all_variables_data = [
            var_info
            for dataset_name, dataset_info in processor.datasets.items()
            for var_info in extract_variable_info(dataset_name, dataset_info)
        ]
        
        total_count = len(all_variables_data)
        
        # 2. 创建DataFrame进行批量处理
        import pandas as pd
        df_variables = pd.DataFrame(all_variables_data)
        
        # 3. 批量查询数据库翻译
        all_variables = [(row['dataset'], row['variable']) for _, row in df_variables.iterrows()]
        batch_translations = db_manager.get_batch_variable_translations(all_variables, ig_version)
        
        # 4. 使用pandas left join进行批量连接
        # 创建翻译结果DataFrame
        translation_data = []
        for (dataset, variable), translation in batch_translations.items():
            translated_label = ''
            if translation_direction == 'en_to_zh':
                translated_label = translation.get('name_cn', '')
            else:
                translated_label = translation.get('name_en', '')
            
            if translated_label:
                translation_data.append({
                    'dataset': dataset,
                    'variable': variable,
                    'translated_label': translated_label,
                    'translation_source': ig_version,
                    'needs_confirmation': False
                })
        
        df_translations = pd.DataFrame(translation_data)
        
        # 5. Left join合并数据，连接不上的输出空值
        if not df_translations.empty:
            df_result = df_variables.merge(
                df_translations, 
                on=['dataset', 'variable'], 
                how='left'
            )
        else:
            df_result = df_variables.copy()
            df_result['translated_label'] = ''
            df_result['translation_source'] = ''
            df_result['needs_confirmation'] = True
        
        # 6. 使用向量化操作处理结果
        df_result['translated_label'] = df_result['translated_label'].fillna('')
        df_result['translation_source'] = df_result['translation_source'].fillna('')
        df_result['needs_confirmation'] = df_result['needs_confirmation'].fillna(True)
        
        # 统计数据库匹配数量
        db_matched_count = len(df_result[df_result['translated_label'] != ''])
        untranslated_count = total_count - db_matched_count
        
        # 7. 使用向量化操作批量保存翻译结果到数据库
        # 只保存有翻译结果的记录
        translated_results = df_result[df_result['translated_label'] != '']
        if not translated_results.empty:
            # 使用apply进行向量化处理
            translated_results.apply(
                lambda row: db_manager.save_translation_result(
                    path, translation_direction, '变量标签', row['dataset'], 
                    row['variable'], row['original_label'], row['translated_label'], 
                    row['translation_source'], row['needs_confirmation']
                ), axis=1
            )
        
        # 8. 转换为最终结果格式
        df_result['needs_confirmation_str'] = df_result['needs_confirmation'].apply(
            lambda x: 'Y' if x else 'N'
        )
        df_result['version'] = ig_version
        
        # 使用向量化操作构建最终结果
        variable_labels = df_result[[
            'dataset', 'variable', 'original_label', 'translated_label', 
            'translation_source', 'needs_confirmation_str', 'version'
        ]].rename(columns={'needs_confirmation_str': 'needs_confirmation'}).to_dict('records')
        
        # 9. 对结果进行排序：数据库翻译在前，按数据集名称和变量名排序
        variable_labels.sort(key=lambda x: (
            0 if x['translation_source'] else 1,  # 有翻译的优先
            x['dataset'],  # 按数据集名称排序
            x['variable']  # 按变量名排序
        ))
        
        result = {
            'success': True,
            'message': '变量Label生成成功',
            'data': {
                'translation_direction': translation_direction,
                'ig_version': ig_version,
                'variable_labels': variable_labels,
                'summary': {
                    'total': total_count,
                    'db_matched': db_matched_count,
                    'untranslated': untranslated_count
                }
            }
        }
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500



# DeepSeek AI翻译服务类
class DeepSeekTranslationService:
    def __init__(self, api_key=None):
        self.api_key = api_key or os.getenv('DEEPSEEK_API_KEY')
        self.base_url = 'https://api.deepseek.com/v1/chat/completions'
        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.api_key}'
        }
    
    def translate_text(self, text, source_lang='en', target_lang='zh', context='medical'):
        """翻译文本"""
        try:
            if not self.api_key:
                raise ValueError('DeepSeek API密钥未配置')
            
            # 构建翻译提示词
            if context == 'medical':
                system_prompt = f"你是一个专业的临床医学SAS数据集翻译专家。请将以下{source_lang}文本准确翻译为{target_lang}，保持医学术语的专业性和准确性。"
            else:
                system_prompt = f"请将以下{source_lang}文本翻译为{target_lang}，保持原意和专业性。"
            
            payload = {
                'model': 'deepseek-chat',
                'messages': [
                    {'role': 'system', 'content': system_prompt},
                    {'role': 'user', 'content': text}
                ],
                'temperature': 0.1,
                'max_tokens': 2000
            }
            
            response = requests.post(self.base_url, headers=self.headers, json=payload, timeout=30)
            response.raise_for_status()
            
            result = response.json()
            translated_text = result['choices'][0]['message']['content'].strip()
            
            return {
                'success': True,
                'translated_text': translated_text,
                'original_text': text,
                'source_lang': source_lang,
                'target_lang': target_lang
            }
            
        except requests.exceptions.RequestException as e:
            return {
                'success': False,
                'error': f'API请求失败: {str(e)}',
                'original_text': text
            }
        except Exception as e:
            return {
                'success': False,
                'error': f'翻译失败: {str(e)}',
                'original_text': text
            }
    
    def batch_translate(self, texts, source_lang='en', target_lang='zh', context='medical'):
        """批量翻译 - 优化版本，真正的批量处理"""
        if not texts:
            return []
        
        # 过滤空文本并记录索引
        valid_texts = []
        text_indices = []
        for i, text in enumerate(texts):
            if text and text.strip():
                valid_texts.append(text.strip())
                text_indices.append(i)
        
        if not valid_texts:
            return [{
                'success': True,
                'translated_text': '',
                'original_text': text,
                'source_lang': source_lang,
                'target_lang': target_lang
            } for text in texts]
        
        try:
            if not self.api_key:
                raise ValueError('DeepSeek API密钥未配置')
            
            # 构建批量翻译提示词
            if context == 'medical':
                system_prompt = f"你是一个专业的临床医学SAS数据集翻译专家。请将以下{source_lang}文本准确翻译为{target_lang}，保持医学术语的专业性和准确性。请按照输入的顺序逐行翻译，每行一个翻译结果，不要添加额外的解释或编号。"
            else:
                system_prompt = f"请将以下{source_lang}文本翻译为{target_lang}，保持原意和专业性。请按照输入的顺序逐行翻译，每行一个翻译结果，不要添加额外的解释或编号。"
            
            # 将多个文本合并为一个请求，用换行符分隔
            batch_text = '\n'.join(valid_texts)
            
            payload = {
                'model': 'deepseek-chat',
                'messages': [
                    {'role': 'system', 'content': system_prompt},
                    {'role': 'user', 'content': batch_text}
                ],
                'temperature': 0.1,
                'max_tokens': 4000  # 增加token限制以支持批量翻译
            }
            
            response = requests.post(self.base_url, headers=self.headers, json=payload, timeout=60)
            response.raise_for_status()
            
            result = response.json()
            translated_batch = result['choices'][0]['message']['content'].strip()
            
            # 分割批量翻译结果
            translated_lines = translated_batch.split('\n')
            
            # 构建结果数组
            results = [None] * len(texts)
            
            # 填充有效翻译结果
            for i, (original_idx, original_text) in enumerate(zip(text_indices, valid_texts)):
                translated_text = translated_lines[i].strip() if i < len(translated_lines) else ''
                results[original_idx] = {
                    'success': True,
                    'translated_text': translated_text,
                    'original_text': original_text,
                    'source_lang': source_lang,
                    'target_lang': target_lang
                }
            
            # 填充空文本结果
            for i, text in enumerate(texts):
                if results[i] is None:
                    results[i] = {
                        'success': True,
                        'translated_text': '',
                        'original_text': text,
                        'source_lang': source_lang,
                        'target_lang': target_lang
                    }
            
            return results
            
        except requests.exceptions.RequestException as e:
            # 如果批量翻译失败，回退到逐个翻译
            print(f"批量翻译失败，回退到逐个翻译: {str(e)}")
            return self._fallback_individual_translate(texts, source_lang, target_lang, context)
        except Exception as e:
            # 如果批量翻译失败，回退到逐个翻译
            print(f"批量翻译异常，回退到逐个翻译: {str(e)}")
            return self._fallback_individual_translate(texts, source_lang, target_lang, context)
    
    def _fallback_individual_translate(self, texts, source_lang='en', target_lang='zh', context='medical'):
        """回退方案：逐个翻译"""
        results = []
        for text in texts:
            if text and text.strip():
                result = self.translate_text(text, source_lang, target_lang, context)
                results.append(result)
            else:
                results.append({
                    'success': True,
                    'translated_text': '',
                    'original_text': text,
                    'source_lang': source_lang,
                    'target_lang': target_lang
                })
        return results

# 创建翻译服务实例
translation_service = DeepSeekTranslationService()

@app.route('/api/translate_text', methods=['POST'])
def translate_text():
    """文本翻译API"""
    try:
        data = request.get_json()
        
        # 支持两种模式：直接翻译文本 或 通过记录ID翻译并更新数据库
        text = data.get('text')
        record_id = data.get('record_id')
        source_lang = data.get('source_lang', 'en')
        target_lang = data.get('target_lang', 'zh')
        context = data.get('context', 'medical')
        save_to_db = data.get('save_to_db', True)  # 新增参数，默认保存到数据库
        
        # 如果提供了record_id，从数据库获取原始值
        if record_id:
            db_manager = DatabaseManager()
            record = db_manager.get_translation_result_by_id(record_id)
            if not record:
                return jsonify({'success': False, 'message': '未找到指定的翻译记录'}), 404
            text = record['original_value']
        
        if not text:
            return jsonify({'success': False, 'message': '缺少要翻译的文本'}), 400
        
        # 调用翻译服务
        result = translation_service.translate_text(text, source_lang, target_lang, context)
        
        # 如果提供了record_id且翻译成功且需要保存到数据库，更新数据库记录
        if record_id and result.get('success') and save_to_db:
            db_manager.update_translation_result(
                record_id,
                translated_value=result.get('translated_text'),
                translation_source='AI',
                needs_confirmation=True
            )
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/batch_translate', methods=['POST'])
def batch_translate():
    """批量翻译API"""
    try:
        data = request.get_json()
        
        texts = data.get('texts', [])
        source_lang = data.get('source_lang', 'en')
        target_lang = data.get('target_lang', 'zh')
        context = data.get('context', 'medical')
        
        if not texts:
            return jsonify({'success': False, 'message': '缺少要翻译的文本列表'}), 400
        
        results = translation_service.batch_translate(texts, source_lang, target_lang, context)
        
        return jsonify({
            'success': True,
            'results': results,
            'total_count': len(texts),
            'success_count': sum(1 for r in results if r.get('success', False))
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/check_translation_service', methods=['GET'])
def check_translation_service():
    """检查翻译服务状态"""
    try:
        if not translation_service.api_key:
            return jsonify({
                'available': False,
                'message': 'DeepSeek API密钥未配置，请设置DEEPSEEK_API_KEY环境变量'
            })
        
        # 测试API连接
        test_result = translation_service.translate_text('test', 'en', 'zh')
        
        return jsonify({
            'available': test_result.get('success', False),
            'message': '翻译服务可用' if test_result.get('success', False) else f"翻译服务不可用: {test_result.get('error', '未知错误')}"
        })
        
    except Exception as e:
        return jsonify({
            'available': False,
            'message': f'检查翻译服务时出错: {str(e)}'
        })

@app.route('/api/translation_results', methods=['GET'])
def get_translation_results():
    """获取翻译结果"""
    try:
        path = request.args.get('path')
        translation_type = request.args.get('translation_type')
        dataset_name = request.args.get('dataset_name')
        variable_name = request.args.get('variable_name')
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 50))
        
        if not path:
            return jsonify({'success': False, 'message': '缺少path参数'}), 400
        
        results = db_manager.get_translation_results(
            path=path,
            translation_type=translation_type,
            dataset_name=dataset_name,
            variable_name=variable_name,
            page=page,
            page_size=page_size
        )
        
        return jsonify({
            'success': True,
            'data': results
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/translation_results/<int:result_id>', methods=['PUT'])
def update_translation_result(result_id):
    """更新翻译结果"""
    try:
        data = request.get_json()
        
        translated_value = data.get('translated_value')
        needs_confirmation = data.get('needs_confirmation')
        is_confirmed = data.get('is_confirmed')
        comments = data.get('comments')
        translation_source = data.get('translation_source', 'AI')  # 默认设置为AI
        
        success = db_manager.update_translation_result(
            result_id=result_id,
            translated_value=translated_value,
            translation_source=translation_source,
            needs_confirmation=needs_confirmation,
            is_confirmed=is_confirmed,
            comments=comments
        )
        
        if success:
            return jsonify({'success': True, 'message': '翻译结果更新成功'})
        else:
            return jsonify({'success': False, 'message': '翻译结果更新失败'}), 400
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/translation_stats', methods=['GET'])
def get_translation_stats():
    """获取翻译统计信息"""
    try:
        path = request.args.get('path')
        
        if not path:
            return jsonify({'success': False, 'message': '缺少path参数'}), 400
        
        stats = db_manager.get_translation_stats(path)
        
        return jsonify({
            'success': True,
            'data': stats
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/translation_check', methods=['POST'])
def perform_translation_check():
    """执行翻译核查"""
    try:
        data = request.get_json()
        path = data.get('path')
        
        if not path:
            return jsonify({'success': False, 'message': '缺少path参数'}), 400
        
        checked_items = db_manager.perform_translation_check(path)
        
        return jsonify({
            'success': True,
            'data': checked_items,
            'message': f'翻译核查完成，共处理 {len(checked_items)} 个问题项目'
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/translation_results/<int:result_id>', methods=['DELETE'])
def delete_translation_result(result_id):
    """删除翻译结果"""
    try:
        success = db_manager.delete_translation_result(result_id)
        
        if success:
            return jsonify({'success': True, 'message': '翻译结果删除成功'})
        else:
            return jsonify({'success': False, 'message': '翻译结果删除失败'}), 400
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/batch_confirm_translations', methods=['POST'])
def batch_confirm_translations():
    """批量确认翻译结果"""
    try:
        data = request.get_json()
        items = data.get('items', [])
        
        if not items:
            return jsonify({'success': False, 'message': '没有要确认的项目'}), 400
        
        success_count = 0
        for item in items:
            result_id = item.get('id')
            translated_value = item.get('translated_value')
            needs_confirmation = item.get('needs_confirmation', False)
            is_confirmed = item.get('is_confirmed', True)
            
            if db_manager.update_translation_result(
                result_id=result_id,
                translated_value=translated_value,
                needs_confirmation=needs_confirmation,
                is_confirmed=is_confirmed
            ):
                success_count += 1
        
        return jsonify({
            'success': True,
            'message': f'批量确认完成，成功确认 {success_count} 个项目'
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/batch_save_translations', methods=['POST'])
def batch_save_translations():
    """批量保存翻译结果"""
    try:
        data = request.get_json()
        items = data.get('items', [])
        
        if not items:
            return jsonify({'success': False, 'message': '没有要保存的项目'}), 400
        
        success_count = 0
        for item in items:
            result_id = item.get('id')
            translated_value = item.get('translated_value')
            translation_source = item.get('translation_source', 'AI')
            needs_confirmation = item.get('needs_confirmation')
            
            if db_manager.update_translation_result(
                result_id=result_id,
                translated_value=translated_value,
                translation_source=translation_source,
                needs_confirmation=needs_confirmation
            ):
                success_count += 1
        
        return jsonify({
            'success': True,
            'message': f'批量保存完成，成功保存 {success_count} 个项目'
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/export_translations', methods=['POST'])
def export_translations():
    """导出翻译数据为xlsx文件"""
    try:
        data = request.get_json()
        path = data.get('path')
        
        if not path:
            return jsonify({'success': False, 'message': '缺少path参数'}), 400
        
        # 获取所有翻译数据
        all_results = db_manager.get_all_translation_results(path)
        
        if not all_results:
            return jsonify({'success': False, 'message': '没有数据可导出'}), 400
        
        # 创建DataFrame
        df_data = []
        for item in all_results:
            df_data.append({
                '编码类型': item.get('translation_type', ''),
                '数据集': item.get('dataset_name', ''),
                '变量': item.get('variable_name', ''),
                '原始值': item.get('original_value', ''),
                '翻译值': item.get('translated_value', ''),
                '翻译来源': item.get('translation_source', ''),
                '备注': item.get('comments', ''),
                '是否确认': '是' if not item.get('needs_confirmation', True) else '否'
            })
        
        df = pd.DataFrame(df_data)
        
        # 创建临时文件
        import tempfile
        import io
        
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='翻译清单', index=False)
        
        output.seek(0)
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name='翻译清单.xlsx'
        )
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
