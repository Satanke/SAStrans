#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MedDRA和WHODrug数据库批量导入工具
Batch Import Tool for MedDRA and WHODrug Databases

这个脚本用于批量导入MedDRA和WHODrug数据到数据库中
支持自动识别目录结构和文件格式
"""

import subprocess
import os
import sys
import re
import pandas as pd
import sqlite3
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time


def run_command(cmd):
    """执行命令并处理输出"""
    try:
        import sys
        encoding = 'mbcs' if sys.platform == 'win32' else 'utf-8'
        
        result = subprocess.run(cmd, capture_output=True)
        
        stdout = result.stdout.decode(encoding, errors='replace') if result.stdout else ""
        stderr = result.stderr.decode(encoding, errors='replace') if result.stderr else ""

        if result.returncode == 0:
            print(f"V 命令执行成功: {' '.join(cmd)}")
            if stdout:
                print(stdout)
        else:
            print(f"X 命令执行失败: {' '.join(cmd)}")
            print(f"  - 返回代码: {result.returncode}")
            print(f"  - STDOUT:\n{stdout}")
            print(f"  - STDERR:\n{stderr}")
            return False
        return True
    except Exception as e:
        print(f"X 执行命令时出错: {e}")
        return False


class ThreadSafeLogger:
    """线程安全的日志输出类"""
    
    def __init__(self):
        self.lock = threading.Lock()
    
    def log(self, message):
        """线程安全的日志输出"""
        with self.lock:
            print(message)
    
    def log_progress(self, current, total, prefix="处理进度"):
        """显示进度"""
        percentage = (current / total) * 100 if total > 0 else 0
        with self.lock:
            print(f"[{prefix}] {current}/{total} ({percentage:.1f}%)")


class MultiThreadImporter:
    """多线程导入管理器"""
    
    def __init__(self, max_workers=4):
        self.max_workers = max_workers
        self.logger = ThreadSafeLogger()
        self.db_lock = threading.Lock()  # 数据库操作锁
    
    def process_meddra_configs_parallel(self, importer, configs):
        """并行处理MedDRA配置"""
        results = {'success': 0, 'failed': 0, 'errors': []}
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交所有任务
            future_to_config = {
                executor.submit(self._process_single_meddra_config, importer, config): config
                for config in configs
            }
            
            # 处理完成的任务
            for future in as_completed(future_to_config):
                config = future_to_config[future]
                try:
                    success = future.result()
                    if success:
                        results['success'] += 1
                        self.logger.log(f"V MedDRA配置完成: {config['table_name']}")
                    else:
                        results['failed'] += 1
                        self.logger.log(f"X MedDRA配置失败: {config['table_name']}")
                except Exception as e:
                    results['failed'] += 1
                    error_msg = f"MedDRA配置异常 {config['table_name']}: {e}"
                    results['errors'].append(error_msg)
                    self.logger.log(f"X {error_msg}")
                
                # 显示进度
                completed = results['success'] + results['failed']
                self.logger.log_progress(completed, len(configs), "MedDRA导入")
        
        return results
    
    def process_whodrug_configs_parallel(self, importer, configs):
        """并行处理WHODrug配置"""
        results = {'success': 0, 'failed': 0, 'errors': []}
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交所有任务
            future_to_config = {
                executor.submit(self._process_single_whodrug_config, importer, config): config
                for config in configs
            }
            
            # 处理完成的任务
            for future in as_completed(future_to_config):
                config = future_to_config[future]
                try:
                    success = future.result()
                    if success:
                        results['success'] += 1
                        self.logger.log(f"V WHODrug配置完成: {config['table_name']}")
                    else:
                        results['failed'] += 1
                        self.logger.log(f"X WHODrug配置失败: {config['table_name']}")
                except Exception as e:
                    results['failed'] += 1
                    error_msg = f"WHODrug配置异常 {config['table_name']}: {e}"
                    results['errors'].append(error_msg)
                    self.logger.log(f"X {error_msg}")
                
                # 显示进度
                completed = results['success'] + results['failed']
                self.logger.log_progress(completed, len(configs), "WHODrug导入")
        
        return results
    
    def _process_single_meddra_config(self, importer, config):
        """处理单个MedDRA配置（线程安全版本）"""
        try:
            # 使用多线程处理文件
            all_data = self._process_meddra_files_parallel(importer, config)
            
            if not all_data:
                return False
            
            # 合并数据
            final_df = pd.concat(all_data, ignore_index=True)
            final_df = final_df.drop_duplicates()
            
            # 线程安全的数据库操作
            return self._save_to_database_threadsafe(final_df, config, 'meddra')
            
        except Exception as e:
            self.logger.log(f"X MedDRA配置处理失败 {config['table_name']}: {e}")
            return False
    
    def _process_single_whodrug_config(self, importer, config):
        """处理单个WHODrug配置（线程安全版本）"""
        try:
            # 使用多线程处理文件
            all_data = self._process_whodrug_files_parallel(importer, config)
            
            if not all_data:
                return False
            
            # 合并数据
            final_df = pd.concat(all_data, ignore_index=True)
            final_df = final_df.drop_duplicates()
            
            # 线程安全的数据库操作
            return self._save_to_database_threadsafe(final_df, config, 'whodrug')
            
        except Exception as e:
            self.logger.log(f"X WHODrug配置处理失败 {config['table_name']}: {e}")
            return False
    
    def _process_meddra_files_parallel(self, importer, config):
        """并行处理MedDRA文件"""
        all_data = []
        
        with ThreadPoolExecutor(max_workers=min(len(importer.meddra_files), 4)) as executor:
            # 提交文件处理任务
            future_to_file = {
                executor.submit(importer.process_meddra_file, 
                              config['data_path'] / filename, 
                              filename.replace('.asc', '')): filename
                for filename in importer.meddra_files
                if (config['data_path'] / filename).exists()
            }
            
            # 收集结果
            for future in as_completed(future_to_file):
                filename = future_to_file[future]
                try:
                    df = future.result()
                    if df is not None and not df.empty:
                        # 添加版本和语言信息
                        df['version'] = config['version']
                        df['language'] = config['language']
                        all_data.append(df)
                except Exception as e:
                    self.logger.log(f"X 文件处理失败 {filename}: {e}")
        
        return all_data
    
    def _process_whodrug_files_parallel(self, importer, config):
        """并行处理WHODrug文件"""
        all_data = []
        
        with ThreadPoolExecutor(max_workers=min(len(importer.whodrug_files), 4)) as executor:
            # 提交文件处理任务
            future_to_file = {
                executor.submit(importer.process_whodrug_file, 
                              config['data_path'] / filename): filename
                for filename in importer.whodrug_files
                if (config['data_path'] / filename).exists()
            }
            
            # 收集结果
            for future in as_completed(future_to_file):
                filename = future_to_file[future]
                try:
                    df = future.result()
                    if df is not None and not df.empty:
                        # 添加版本和语言信息
                        df['version'] = config['version']
                        df['language'] = config['language']
                        all_data.append(df)
                except Exception as e:
                    self.logger.log(f"X 文件处理失败 {filename}: {e}")
        
        return all_data
    
    def _save_to_database_threadsafe(self, df, config, category):
        """线程安全的数据库保存"""
        with self.db_lock:
            try:
                # 保存到临时文件
                temp_file = f"temp_{category}_{config['table_name']}_{threading.current_thread().ident}.pkl"
                df.to_pickle(temp_file)
                
                # 使用database_setup.py导入
                cmd = [
                    'python', 'database_setup.py', 'import',
                    temp_file,
                    config['table_name'],
                    config['category'],
                    '--desc', config['description'],
                    '--version', config['version']
                ]
                
                success = run_command(cmd)
                
                # 清理临时文件
                try:
                    os.remove(temp_file)
                except:
                    pass
                
                return success
                
            except Exception as e:
                self.logger.log(f"X 数据库保存失败: {e}")
                return False


class MedDRAImporter:
    """MedDRA数据导入器"""
    
    def __init__(self, root_path):
        self.root_path = Path(root_path)
        self.meddra_files = ['hlgt.asc', 'hlt.asc', 'llt.asc', 'pt.asc', 'soc.asc']
    
    def scan_meddra_directories(self):
        """扫描MedDRA目录结构"""
        print(f"\n[+] 扫描MedDRA目录: {self.root_path}")
        
        if not self.root_path.exists():
            print(f"X 目录不存在: {self.root_path}")
            return []
        
        meddra_configs = []
        
        # 扫描子目录
        for subdir in self.root_path.iterdir():
            if not subdir.is_dir():
                continue
            
            # 使用正则表达式解析目录名
            # 例如: meddra_18_0_chinese, meddra_18_0_english
            match = re.match(r'^(meddra)_(\d+_\d+)_(chinese|english)$', subdir.name.lower())
            if not match:
                print(f"[*] 跳过非标准目录: {subdir.name}")
                continue
            
            category = match.group(1)  # meddra
            version_str = match.group(2)  # 18_0
            language = match.group(3)  # chinese/english
            
            # 转换版本格式
            version = version_str.replace('_', '.')  # 18.0
            
            # 确定数据文件路径
            if language == 'english':
                data_path = subdir / 'MedAscii'
            else:  # chinese
                # 查找ascii-开头的文件夹
                ascii_dirs = list(subdir.glob('ascii-*'))
                if not ascii_dirs:
                    print(f"X 在{subdir.name}中未找到ascii-开头的目录")
                    continue
                data_path = ascii_dirs[0]
            
            if not data_path.exists():
                print(f"X 数据目录不存在: {data_path}")
                continue
            
            # 检查必需的文件
            missing_files = []
            for filename in self.meddra_files:
                if not (data_path / filename).exists():
                    missing_files.append(filename)
            
            if missing_files:
                print(f"[*] {subdir.name} 缺少文件: {missing_files}")
                continue
            
            config = {
                'subdir_name': subdir.name,
                'category': category,
                'version': version,
                'language': language,
                'data_path': data_path,
                'table_name': subdir.name.lower(),
                'description': f"MedDRA {version} {language.title()}"
            }
            
            meddra_configs.append(config)
            print(f"V 发现MedDRA配置: {config['table_name']} - {config['description']}")
        
        return meddra_configs
    
    def process_meddra_file(self, file_path, file_type):
        """处理单个MedDRA文件"""
        print(f"  - 处理文件: {file_path.name}")
        
        try:
            # 读取ASC文件，使用$作为分隔符
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            data = []
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                
                parts = line.split('$')
                if len(parts) >= 2:
                    code = parts[0].strip()
                    name = parts[1].strip()
                    data.append({
                        'code': code,
                        'name': name,
                        'source': file_type  # 数据来源，如llt, pt, hlgt等
                    })
            
            return pd.DataFrame(data)
            
        except Exception as e:
            print(f"X 处理文件失败 {file_path}: {e}")
            return None
    
    def import_meddra_data(self, config):
        """导入单个MedDRA配置的数据"""
        print(f"\n[IMPORT] 导入MedDRA数据: {config['table_name']}")
        
        all_data = []
        
        # 处理每个文件
        for filename in self.meddra_files:
            file_path = config['data_path'] / filename
            file_type = filename.replace('.asc', '')  # hlgt, hlt, llt, pt, soc
            
            df = self.process_meddra_file(file_path, file_type)
            if df is not None and not df.empty:
                # 添加版本和语言信息
                df['version'] = config['version']
                df['language'] = config['language']
                all_data.append(df)
            else:
                print(f"[*] 文件 {filename} 处理失败或为空")
        
        if not all_data:
            print(f"X 没有可导入的数据: {config['table_name']}")
            return False
        
        # 合并所有数据
        try:
            # 将所有文件的数据合并到一个DataFrame中
            final_df = pd.concat(all_data, ignore_index=True)
            
            # 去重
            final_df = final_df.drop_duplicates()
            print(f"合并后共 {len(final_df)} 条记录")
            
            # 保存到临时文件
            temp_file = f"temp_meddra_{config['table_name']}.pkl"
            final_df.to_pickle(temp_file)
            
            # 使用database_setup.py导入
            cmd = [
                'python', 'database_setup.py', 'import',
                temp_file,
                config['table_name'],
                config['category'],
                '--desc', config['description'],
                '--version', config['version']
            ]
            
            success = run_command(cmd)
            
            # 清理临时文件
            try:
                os.remove(temp_file)
            except:
                pass
            
            return success
            
        except Exception as e:
            print(f"X 数据合并失败: {e}")
            return False


class WHODrugImporter:
    """WHODrug数据导入器"""
    
    def __init__(self, root_path):
        self.root_path = Path(root_path)
        self.whodrug_files = ['INA.txt', 'DD.txt']
    
    def scan_whodrug_directories(self):
        """扫描WHODrug目录结构"""
        print(f"\n[+] 扫描WHODrug目录: {self.root_path}")
        
        if not self.root_path.exists():
            print(f"X 目录不存在: {self.root_path}")
            return []
        
        whodrug_configs = []
        
        # 扫描子目录
        for subdir in self.root_path.iterdir():
            if not subdir.is_dir():
                continue
            
            # 使用正则表达式解析目录名
            # 例如: WHODrug Global 2023 Mar 1, WHODrug Global 2023 Sep 1
            match = re.match(r'^(WHODrug)\s+Global\s+(.+)$', subdir.name, re.IGNORECASE)
            if not match:
                print(f"[*] 跳过非标准目录: {subdir.name}")
                continue
            
            category = match.group(1).lower()  # whodrug
            version_str = match.group(2)  # 2023 Mar 1
            
            # 查找英文和中文数据目录
            english_dir = subdir / 'WHODrug B3_C3-format'
            chinese_dirs = list(subdir.glob('WHODrug Global Chinese txt_*'))
            
            # 处理英文版本
            if english_dir.exists():
                b3_dirs = list(english_dir.glob('whodrug_global_b3_*'))
                if b3_dirs:
                    data_path = b3_dirs[0]
                    if self._check_whodrug_files(data_path):
                        config = {
                            'subdir_name': subdir.name,
                            'category': category,
                            'version': version_str,
                            'language': 'english',
                            'data_path': data_path,
                            'table_name': f"{subdir.name.lower().replace(' ', '_')}_english",
                            'description': f"WHODrug {version_str} English"
                        }
                        whodrug_configs.append(config)
                        print(f"V 发现WHODrug英文配置: {config['table_name']}")
            
            # 处理中文版本
            if chinese_dirs:
                chinese_dir = chinese_dirs[0]
                b3_dirs = list(chinese_dir.glob('whodrug_global_b3_*'))
                if b3_dirs:
                    data_path = b3_dirs[0]
                    if self._check_whodrug_files(data_path):
                        config = {
                            'subdir_name': subdir.name,
                            'category': category,
                            'version': version_str,
                            'language': 'chinese',
                            'data_path': data_path,
                            'table_name': f"{subdir.name.lower().replace(' ', '_')}_chinese",
                            'description': f"WHODrug {version_str} Chinese"
                        }
                        whodrug_configs.append(config)
                        print(f"V 发现WHODrug中文配置: {config['table_name']}")
        
        return whodrug_configs
    
    def _check_whodrug_files(self, data_path):
        """检查WHODrug必需文件是否存在"""
        for filename in self.whodrug_files:
            if not (data_path / filename).exists():
                print(f"X 缺少文件: {data_path / filename}")
                return False
        return True
    
    def process_whodrug_file(self, file_path):
        """处理单个WHODrug文件"""
        print(f"  - 处理文件: {file_path.name}")
        
        try:
            # 读取TXT文件
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            # 确定source字段（从文件名推导）
            file_type = file_path.stem.lower()  # 如ina, dd
            
            data = []
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                
                if 'ina' in file_type.lower():
                    # INA.txt特殊处理：第一个空格前为code，第八个字符为level，第八个字符后为name
                    space_index = line.find(' ')
                    if space_index > 0 and len(line) > 8:
                        code = line[:space_index].strip()
                        level = line[7] if len(line) > 7 else ''  # 第八个字符（索引7）
                        name = line[8:].strip() if len(line) > 8 else ''  # 第八个字符之后
                        
                        data.append({
                            'code': code,
                            'name': name,
                            'level': level,
                            'source': file_type
                        })
                    else:
                        # 如果格式不符合预期，使用备用处理方式
                        parts = line.split()
                        if len(parts) >= 2:
                            code = parts[0].strip()
                            name = ' '.join(parts[1:]).strip()
                            data.append({
                                'code': code,
                                'name': name,
                                'level': '',
                                'source': file_type
                            })
                else:
                    # 其他文件（如DD.txt）使用原有处理方式
                    parts = line.split()
                    if len(parts) >= 2:
                        code = parts[0].strip()
                        name = ' '.join(parts[1:]).strip()
                        data.append({
                            'code': code,
                            'name': name,
                            'level': '',  # 非INA文件level为空
                            'source': file_type
                        })
            
            return pd.DataFrame(data)
            
        except Exception as e:
            print(f"X 处理文件失败 {file_path}: {e}")
            return None
    
    def import_whodrug_data(self, config):
        """导入单个WHODrug配置的数据"""
        print(f"\n[IMPORT] 导入WHODrug数据: {config['table_name']}")
        
        all_data = []
        
        # 处理每个文件
        for filename in self.whodrug_files:
            file_path = config['data_path'] / filename
            
            df = self.process_whodrug_file(file_path)
            if df is not None and not df.empty:
                # 添加版本和语言信息
                df['version'] = config['version']
                df['language'] = config['language']
                all_data.append(df)
            else:
                print(f"[*] 文件 {filename} 处理失败或为空")
        
        if not all_data:
            print(f"X 没有可导入的数据: {config['table_name']}")
            return False
        
        # 合并所有数据
        try:
            final_df = pd.concat(all_data, ignore_index=True)
            
            # 去重
            final_df = final_df.drop_duplicates()
            print(f"合并后共 {len(final_df)} 条记录")
            
            # 保存到临时文件
            temp_file = f"temp_whodrug_{config['table_name']}.pkl"
            final_df.to_pickle(temp_file)
            
            # 使用database_setup.py导入
            cmd = [
                'python', 'database_setup.py', 'import',
                temp_file,
                config['table_name'],
                config['category'],
                '--desc', config['description'],
                '--version', config['version']
            ]
            
            success = run_command(cmd)
            
            # 清理临时文件
            try:
                os.remove(temp_file)
            except:
                pass
            
            return success
            
        except Exception as e:
            print(f"X 数据合并失败: {e}")
            return False


class DataMerger:
    """数据合成器"""
    
    def __init__(self, db_path='translation_db.sqlite'):
        self.db_path = db_path
    
    def create_merged_meddra_table(self):
        """创建MedDRA合成表"""
        print("\n[MERGE] 创建MedDRA合成表...")
        
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            
            # 获取所有MedDRA表
            cursor.execute('SELECT name FROM meddra_tables')
            meddra_table_names = [row[0] for row in cursor.fetchall()]
            
            if not meddra_table_names:
                print("X 没有找到MedDRA表")
                return False
            
            all_data = []
            
            for table_name in meddra_table_names:
                try:
                    # 读取表数据
                    df = pd.read_sql_query(f'SELECT * FROM "{table_name}"', conn)
                    
                    if not df.empty and 'code' in df.columns and 'name' in df.columns:
                        all_data.append(df)
                        print(f"  - 读取表 {table_name}: {len(df)} 条记录")
                    else:
                        print(f"  - 跳过表 {table_name}: 缺少必要字段或为空")
                        
                except Exception as e:
                    print(f"  - 读取表 {table_name} 失败: {e}")
            
            if not all_data:
                print("X 没有可合成的MedDRA数据")
                return False
            
            # 合并所有数据
            merged_df = pd.concat(all_data, ignore_index=True)
            
            # 创建合成表结构
            final_data = []
            
            # 按精确字段组合分组（MedDRA没有level字段）
            group_fields = ['code', 'version', 'source']
            grouped = merged_df.groupby(group_fields)
            
            for group_key, group in grouped:
                code, version, source = group_key
                
                # 分离中英文名称
                chinese_names = group[group['language'] == 'chinese']['name'].unique()
                english_names = group[group['language'] == 'english']['name'].unique()
                
                # 取第一个有效的中英文名称
                name_cn = chinese_names[0] if len(chinese_names) > 0 else ''
                name_en = english_names[0] if len(english_names) > 0 else ''
                
                # 如果某种语言的名称缺失，尝试从同code的其他记录中获取
                if not name_cn or not name_en:
                    same_code_records = merged_df[merged_df['code'] == code]
                    if not name_cn and len(same_code_records[same_code_records['language'] == 'chinese']) > 0:
                        name_cn = same_code_records[same_code_records['language'] == 'chinese']['name'].iloc[0]
                    if not name_en and len(same_code_records[same_code_records['language'] == 'english']) > 0:
                        name_en = same_code_records[same_code_records['language'] == 'english']['name'].iloc[0]
                
                final_data.append({
                    'code': code,
                    'name_cn': name_cn,
                    'name_en': name_en,
                    'version': version,
                    'source': source
                })
            
            # 创建最终DataFrame
            final_df = pd.DataFrame(final_data)
            final_df = final_df.drop_duplicates()
            
            # 保存到数据库
            final_df.to_sql('meddra_merged', conn, if_exists='replace', index=False)
            
            # 更新meddra_tables记录
            cursor.execute('''
                INSERT OR REPLACE INTO meddra_tables 
                (name, description, record_count, version, updated_at)
                VALUES (?, ?, ?, ?, ?)
            ''', ('meddra_merged', 'MedDRA合成表', len(final_df), 'all_versions', datetime.now()))
            
            conn.commit()
            print(f"V MedDRA合成表创建完成: {len(final_df)} 条记录")
            
            return True
            
        except Exception as e:
            print(f"X MedDRA合成失败: {e}")
            return False
        finally:
            conn.close()
    
    def create_merged_whodrug_table(self):
        """创建WHODrug合成表"""
        print("\n[MERGE] 创建WHODrug合成表...")
        
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            
            # 获取所有WHODrug表
            cursor.execute('SELECT name FROM whodrug_tables')
            whodrug_table_names = [row[0] for row in cursor.fetchall()]
            
            if not whodrug_table_names:
                print("X 没有找到WHODrug表")
                return False
            
            all_data = []
            
            for table_name in whodrug_table_names:
                try:
                    # 读取表数据
                    df = pd.read_sql_query(f'SELECT * FROM "{table_name}"', conn)
                    
                    if not df.empty and 'code' in df.columns and 'name' in df.columns:
                        all_data.append(df)
                        print(f"  - 读取表 {table_name}: {len(df)} 条记录")
                    else:
                        print(f"  - 跳过表 {table_name}: 缺少必要字段或为空")
                        
                except Exception as e:
                    print(f"  - 读取表 {table_name} 失败: {e}")
            
            if not all_data:
                print("X 没有可合成的WHODrug数据")
                return False
            
            # 合并所有数据
            merged_df = pd.concat(all_data, ignore_index=True)
            
            # 创建合成表结构
            final_data = []
            
            # 构建分组字段列表
            group_fields = ['code', 'version', 'source']
            if 'level' in merged_df.columns:
                group_fields.append('level')
            
            # 按精确字段组合分组
            grouped = merged_df.groupby(group_fields)
            
            for group_key, group in grouped:
                # 提取分组键值
                if len(group_fields) == 4:  # 包含level
                    code, version, source, level = group_key
                else:  # 不包含level
                    code, version, source = group_key
                    level = ''
                
                # 分离中英文名称
                chinese_names = group[group['language'] == 'chinese']['name'].unique()
                english_names = group[group['language'] == 'english']['name'].unique()
                
                # 取第一个有效的中英文名称
                name_cn = chinese_names[0] if len(chinese_names) > 0 else ''
                name_en = english_names[0] if len(english_names) > 0 else ''
                
                # 如果某种语言的名称缺失，尝试从同code的其他记录中获取
                if not name_cn or not name_en:
                    same_code_records = merged_df[merged_df['code'] == code]
                    if not name_cn and len(same_code_records[same_code_records['language'] == 'chinese']) > 0:
                        name_cn = same_code_records[same_code_records['language'] == 'chinese']['name'].iloc[0]
                    if not name_en and len(same_code_records[same_code_records['language'] == 'english']) > 0:
                        name_en = same_code_records[same_code_records['language'] == 'english']['name'].iloc[0]
                
                final_data.append({
                    'code': code,
                    'name_cn': name_cn,
                    'name_en': name_en,
                    'version': version,
                    'source': source,
                    'level': level
                })
            
            # 创建最终DataFrame
            final_df = pd.DataFrame(final_data)
            final_df = final_df.drop_duplicates()
            
            # 保存到数据库
            final_df.to_sql('whodrug_merged', conn, if_exists='replace', index=False)
            
            # 更新whodrug_tables记录
            cursor.execute('''
                INSERT OR REPLACE INTO whodrug_tables 
                (name, description, record_count, version, updated_at)
                VALUES (?, ?, ?, ?, ?)
            ''', ('whodrug_merged', 'WHODrug合成表', len(final_df), 'all_versions', datetime.now()))
            
            conn.commit()
            print(f"V WHODrug合成表创建完成: {len(final_df)} 条记录")
            
            return True
            
        except Exception as e:
            print(f"X WHODrug合成失败: {e}")
            return False
        finally:
            conn.close()


def check_existing_tables(db_path='translation_db.sqlite'):
    """检查数据库中已存在的表"""
    existing_tables = {'meddra': set(), 'whodrug': set()}
    
    if not Path(db_path).exists():
        print("数据库文件不存在，将进行全新导入")
        return existing_tables
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 检查MedDRA表
        cursor.execute('SELECT name FROM meddra_tables')
        meddra_tables = cursor.fetchall()
        for row in meddra_tables:
            existing_tables['meddra'].add(row[0])
        
        # 检查WHODrug表
        cursor.execute('SELECT name FROM whodrug_tables')
        whodrug_tables = cursor.fetchall()
        for row in whodrug_tables:
            existing_tables['whodrug'].add(row[0])
        
        conn.close()
        
        if existing_tables['meddra'] or existing_tables['whodrug']:
            print(f"发现已存在的表: MedDRA {len(existing_tables['meddra'])}个, WHODrug {len(existing_tables['whodrug'])}个")
        
        return existing_tables
        
    except Exception as e:
        print(f"检查已存在表时出错: {e}")
        return existing_tables


def filter_existing_configs(configs, existing_tables, category):
    """过滤掉已存在的配置"""
    if not existing_tables.get(category):
        return configs
    
    filtered_configs = []
    skipped_count = 0
    
    for config in configs:
        table_name = config['table_name']
        if table_name not in existing_tables[category]:
            filtered_configs.append(config)
        else:
            skipped_count += 1
            print(f"[SKIP] 表已存在，跳过: {table_name}")
    
    if skipped_count > 0:
        print(f"跳过了 {skipped_count} 个已存在的{category.upper()}表")
    
    return filtered_configs


def get_user_input():
    """获取用户输入的路径配置"""
    print("请输入MedDRA和WHODrug的根目录路径：")
    print("(如果路径不存在或输入为空，将跳过对应的导入)\n")
    
    # 获取MedDRA路径
    meddra_path = input("MedDRA根目录路径 (例如: \\\\192.168.123.22\\z\\Public\\DataCenter\\Coding\\MedDRA): ").strip()
    if not meddra_path:
        print("[*] MedDRA路径为空，将跳过MedDRA导入")
    
    # 获取WHODrug路径
    whodrug_path = input("WHODrug根目录路径 (例如: \\\\192.168.123.22\\z\\Public\\DataCenter\\Coding\\WHODrug): ").strip()
    if not whodrug_path:
        print("[*] WHODrug路径为空，将跳过WHODrug导入")
    
    return meddra_path, whodrug_path


def get_initialization_choice():
    """获取用户关于数据库初始化的选择"""
    print("\n选择数据库初始化模式：")
    print("1. 重新初始化数据库（清空所有数据）")
    print("2. 增量导入（只导入不存在的表）")
    
    while True:
        choice = input("请选择 (1/2) [默认为2]: ").strip()
        
        if not choice or choice == '2':
            return 'incremental'
        elif choice == '1':
            return 'reinitialize'
        else:
            print("请输入有效选择 (1 或 2)")


def initialize_database_conditionally(mode):
    """根据模式条件性地初始化数据库"""
    if mode == 'reinitialize':
        print("1. 重新初始化数据库结构...")
        if not run_command(['python', 'database_setup.py', 'init']):
            print("数据库初始化失败，退出")
            sys.exit(1)
        return True
    elif mode == 'incremental':
        print("1. 检查数据库状态...")
        # 检查数据库是否存在表结构
        db_path = 'translation_db.sqlite'
        if not Path(db_path).exists():
            print("数据库不存在，创建新数据库...")
            if not run_command(['python', 'database_setup.py', 'init']):
                print("数据库初始化失败，退出")
                sys.exit(1)
            return True
        else:
            # 检查表结构是否完整
            try:
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = [row[0] for row in cursor.fetchall()]
                conn.close()
                
                expected_tables = ['meddra_tables', 'whodrug_tables']
                missing_tables = [t for t in expected_tables if t not in tables]
                
                if missing_tables:
                    print(f"数据库表结构不完整，缺少表: {missing_tables}")
                    print("重新初始化数据库结构...")
                    if not run_command(['python', 'database_setup.py', 'init']):
                        print("数据库初始化失败，退出")
                        sys.exit(1)
                    return True
                else:
                    print("数据库结构完整，准备增量导入...")
                    return False
            except Exception as e:
                print(f"检查数据库时出错: {e}")
                print("重新初始化数据库...")
                if not run_command(['python', 'database_setup.py', 'init']):
                    print("数据库初始化失败，退出")
                    sys.exit(1)
                return True
    
    return False


def main_multithread(max_workers=4):
    """多线程主函数"""
    print("=== MedDRA和WHODrug数据库批量导入工具（多线程版本）===\n")
    print(f"使用 {max_workers} 个并行线程\n")
    
    # 检查database_setup.py是否存在
    if not Path('database_setup.py').exists():
        print("X 错误: database_setup.py 文件不存在")
        print("请确保在正确的项目目录中运行此脚本")
        sys.exit(1)
    
    # 创建多线程导入器
    mt_importer = MultiThreadImporter(max_workers=max_workers)
    
    # 1. 获取用户关于数据库初始化的选择
    init_mode = get_initialization_choice()
    
    # 2. 根据选择初始化数据库
    was_reinitialized = initialize_database_conditionally(init_mode)
    
    print("\n2. 检查数据库状态...")
    run_command(['python', 'database_setup.py', 'status'])
    
    # 3. 检查已存在的表（如果是增量模式）
    existing_tables = {'meddra': set(), 'whodrug': set()}
    if init_mode == 'incremental' and not was_reinitialized:
        print("\n3. 检查已存在的表...")
        existing_tables = check_existing_tables()
    
    # 4. 获取用户输入的路径
    print("\n4. 配置导入路径...")
    meddra_path, whodrug_path = get_user_input()
    
    total_success = 0
    total_fail = 0
    total_skip = 0
    
    start_time = time.time()
    
    # 5. 并行处理MedDRA和WHODrug数据
    meddra_future = None
    whodrug_future = None
    
    with ThreadPoolExecutor(max_workers=2) as main_executor:
        # 同时启动MedDRA和WHODrug处理
        if meddra_path:
            print("\n5. 开始处理MedDRA数据（多线程）...")
            meddra_future = main_executor.submit(process_meddra_multithread, 
                                               mt_importer, meddra_path, existing_tables)
        
        if whodrug_path:
            print("\n6. 开始处理WHODrug数据（多线程）...")
            whodrug_future = main_executor.submit(process_whodrug_multithread, 
                                                 mt_importer, whodrug_path, existing_tables)
        
        # 等待MedDRA完成
        if meddra_future:
            try:
                meddra_results = meddra_future.result()
                total_success += meddra_results['success']
                total_fail += meddra_results['failed']
                print(f"\nMedDRA处理完成: 成功{meddra_results['success']}个，失败{meddra_results['failed']}个")
            except Exception as e:
                print(f"X MedDRA处理异常: {e}")
                total_fail += 1
        else:
            total_skip += 1
        
        # 等待WHODrug完成
        if whodrug_future:
            try:
                whodrug_results = whodrug_future.result()
                total_success += whodrug_results['success']
                total_fail += whodrug_results['failed']
                print(f"\nWHODrug处理完成: 成功{whodrug_results['success']}个，失败{whodrug_results['failed']}个")
            except Exception as e:
                print(f"X WHODrug处理异常: {e}")
                total_fail += 1
        else:
            total_skip += 1
    
    processing_time = time.time() - start_time
    
    # 6. 显示导入结果
    print(f"\n=== 批量导入完成（耗时: {processing_time:.2f}秒）===")
    print(f"V 成功导入: {total_success}个表")
    print(f"[*] 跳过处理: {total_skip}个类别")
    print(f"X 导入失败: {total_fail}个表")
    
    # 8. 创建/更新合成表
    # 检查是否需要创建合成表
    need_merge = False
    merge_reason = ""
    
    if total_success > 0:
        need_merge = True
        merge_reason = f"有 {total_success} 个新表导入"
    elif existing_tables['meddra'] or existing_tables['whodrug']:
        need_merge = True
        merge_reason = "检测到已有数据表，确保合成表为最新"
    
    if need_merge:
        print(f"\n7. 创建版本合成表...（{merge_reason}）")
        merge_start_time = time.time()
        
        data_merger = DataMerger()
        
        # 并行创建合成表
        with ThreadPoolExecutor(max_workers=2) as merge_executor:
            futures = []
            
            # 只为有数据的类型创建合成表
            if existing_tables['meddra']:
                futures.append(('MedDRA', merge_executor.submit(data_merger.create_merged_meddra_table)))
            
            if existing_tables['whodrug']:
                futures.append(('WHODrug', merge_executor.submit(data_merger.create_merged_whodrug_table)))
            
            # 等待合成完成
            for table_type, future in futures:
                try:
                    future.result()
                    print(f"V {table_type}合成表创建成功")
                except Exception as e:
                    print(f"X {table_type}合成表创建失败: {e}")
        
        merge_time = time.time() - merge_start_time
        print(f"合成表创建完成（耗时: {merge_time:.2f}秒）")
    else:
        print("\n7. 跳过合成表创建（没有发现任何数据表）")
    
    # 9. 显示最终数据库状态
    print("\n8. 最终数据库状态:")
    run_command(['python', 'database_setup.py', 'list'])
    
    total_time = time.time() - start_time
    print(f"\n=== 多线程批量导入工具运行完成（总耗时: {total_time:.2f}秒）===")
    print("现在可以运行主应用程序: python app.py")


def process_meddra_multithread(mt_importer, meddra_path, existing_tables=None):
    """多线程处理MedDRA数据"""
    meddra_importer = MedDRAImporter(meddra_path)
    meddra_configs = meddra_importer.scan_meddra_directories()
    
    if meddra_configs:
        print(f"找到 {len(meddra_configs)} 个MedDRA配置")
        
        # 过滤已存在的配置
        if existing_tables:
            meddra_configs = filter_existing_configs(meddra_configs, existing_tables, 'meddra')
            print(f"过滤后剩余 {len(meddra_configs)} 个MedDRA配置需要导入")
        
        if meddra_configs:
            return mt_importer.process_meddra_configs_parallel(meddra_importer, meddra_configs)
        else:
            print("所有MedDRA表都已存在，跳过导入")
            return {'success': 0, 'failed': 0, 'errors': []}
    else:
        print("没有找到有效的MedDRA配置")
        return {'success': 0, 'failed': 0, 'errors': []}


def process_whodrug_multithread(mt_importer, whodrug_path, existing_tables=None):
    """多线程处理WHODrug数据"""
    whodrug_importer = WHODrugImporter(whodrug_path)
    whodrug_configs = whodrug_importer.scan_whodrug_directories()
    
    if whodrug_configs:
        print(f"找到 {len(whodrug_configs)} 个WHODrug配置")
        
        # 过滤已存在的配置
        if existing_tables:
            whodrug_configs = filter_existing_configs(whodrug_configs, existing_tables, 'whodrug')
            print(f"过滤后剩余 {len(whodrug_configs)} 个WHODrug配置需要导入")
        
        if whodrug_configs:
            return mt_importer.process_whodrug_configs_parallel(whodrug_importer, whodrug_configs)
        else:
            print("所有WHODrug表都已存在，跳过导入")
            return {'success': 0, 'failed': 0, 'errors': []}
    else:
        print("没有找到有效的WHODrug配置")
        return {'success': 0, 'failed': 0, 'errors': []}


def main():
    """主函数（单线程版本）"""
    print("=== MedDRA和WHODrug数据库批量导入工具（单线程版本）===\n")
    
    # 检查database_setup.py是否存在
    if not Path('database_setup.py').exists():
        print("X 错误: database_setup.py 文件不存在")
        print("请确保在正确的项目目录中运行此脚本")
        sys.exit(1)
    
    # 1. 获取用户关于数据库初始化的选择
    init_mode = get_initialization_choice()
    
    # 2. 根据选择初始化数据库
    was_reinitialized = initialize_database_conditionally(init_mode)
    
    print("\n2. 检查数据库状态...")
    run_command(['python', 'database_setup.py', 'status'])
    
    # 3. 检查已存在的表（如果是增量模式）
    existing_tables = {'meddra': set(), 'whodrug': set()}
    if init_mode == 'incremental' and not was_reinitialized:
        print("\n3. 检查已存在的表...")
        existing_tables = check_existing_tables()
    
    # 4. 获取用户输入的路径
    print("\n4. 配置导入路径...")
    meddra_path, whodrug_path = get_user_input()
    
    total_success = 0
    total_skip = 0
    total_fail = 0
    
    # 5. 处理MedDRA数据
    if meddra_path:
        print("\n5. 开始处理MedDRA数据...")
        meddra_importer = MedDRAImporter(meddra_path)
        meddra_configs = meddra_importer.scan_meddra_directories()
        
        if meddra_configs:
            print(f"找到 {len(meddra_configs)} 个MedDRA配置")
            
            # 过滤已存在的配置
            if existing_tables:
                meddra_configs = filter_existing_configs(meddra_configs, existing_tables, 'meddra')
                print(f"过滤后剩余 {len(meddra_configs)} 个MedDRA配置需要导入")
            
            for i, config in enumerate(meddra_configs, 1):
                print(f"\n--- 处理MedDRA配置 {i}/{len(meddra_configs)} ---")
                print(f"表名: {config['table_name']}")
                print(f"版本: {config['version']}")
                print(f"语言: {config['language']}")
                print(f"路径: {config['data_path']}")
                
                try:
                    if meddra_importer.import_meddra_data(config):
                        total_success += 1
                        print(f"V MedDRA导入成功: {config['table_name']}")
                    else:
                        total_fail += 1
                        print(f"X MedDRA导入失败: {config['table_name']}")
                except Exception as e:
                    total_fail += 1
                    print(f"X MedDRA导入异常 {config['table_name']}: {e}")
        else:
            print("没有找到有效的MedDRA配置")
            total_skip += 1
    else:
        print("\n5. 跳过MedDRA数据处理（路径未提供）")
        total_skip += 1
    
    # 6. 处理WHODrug数据
    if whodrug_path:
        print("\n6. 开始处理WHODrug数据...")
        whodrug_importer = WHODrugImporter(whodrug_path)
        whodrug_configs = whodrug_importer.scan_whodrug_directories()
        
        if whodrug_configs:
            print(f"找到 {len(whodrug_configs)} 个WHODrug配置")
            
            # 过滤已存在的配置
            if existing_tables:
                whodrug_configs = filter_existing_configs(whodrug_configs, existing_tables, 'whodrug')
                print(f"过滤后剩余 {len(whodrug_configs)} 个WHODrug配置需要导入")
            
            for i, config in enumerate(whodrug_configs, 1):
                print(f"\n--- 处理WHODrug配置 {i}/{len(whodrug_configs)} ---")
                print(f"表名: {config['table_name']}")
                print(f"版本: {config['version']}")
                print(f"语言: {config['language']}")
                print(f"路径: {config['data_path']}")
                
                try:
                    if whodrug_importer.import_whodrug_data(config):
                        total_success += 1
                        print(f"V WHODrug导入成功: {config['table_name']}")
                    else:
                        total_fail += 1
                        print(f"X WHODrug导入失败: {config['table_name']}")
                except Exception as e:
                    total_fail += 1
                    print(f"X WHODrug导入异常 {config['table_name']}: {e}")
        else:
            print("没有找到有效的WHODrug配置")
            total_skip += 1
    else:
        print("\n6. 跳过WHODrug数据处理（路径未提供）")
        total_skip += 1
    
    # 7. 显示导入结果
    print(f"\n=== 批量导入完成 ===")
    print(f"V 成功导入: {total_success}个表")
    print(f"[*] 跳过处理: {total_skip}个类别")
    print(f"X 导入失败: {total_fail}个表")
    
    # 8. 创建/更新合成表
    # 检查是否需要创建合成表
    need_merge = False
    merge_reason = ""
    
    if total_success > 0:
        need_merge = True
        merge_reason = f"有 {total_success} 个新表导入"
    elif existing_tables['meddra'] or existing_tables['whodrug']:
        need_merge = True
        merge_reason = "检测到已有数据表，确保合成表为最新"
    
    if need_merge:
        print(f"\n7. 创建版本合成表...（{merge_reason}）")
        data_merger = DataMerger()
        
        # 只为有数据的类型创建合成表
        if existing_tables['meddra']:
            try:
                data_merger.create_merged_meddra_table()
                print("V MedDRA合成表创建成功")
            except Exception as e:
                print(f"X MedDRA合成表创建失败: {e}")
        
        if existing_tables['whodrug']:
            try:
                data_merger.create_merged_whodrug_table()
                print("V WHODrug合成表创建成功")
            except Exception as e:
                print(f"X WHODrug合成表创建失败: {e}")
    else:
        print("\n7. 跳过合成表创建（没有发现任何数据表）")
    
    # 9. 显示最终数据库状态
    print("\n8. 最终数据库状态:")
    run_command(['python', 'database_setup.py', 'list'])
    
    print("\n=== 批量导入工具运行完成 ===")
    print("现在可以运行主应用程序: python app.py")


def demo_mode():
    """演示模式 - 使用示例路径"""
    print("=== 演示模式 ===")
    print("使用示例路径进行测试...\n")
    
    # 示例路径
    demo_meddra_path = r"\\192.168.123.22\z\Public\DataCenter\Coding\MedDRA"
    demo_whodrug_path = r"\\192.168.123.22\z\Public\DataCenter\Coding\WHODrug"
    
    print(f"MedDRA示例路径: {demo_meddra_path}")
    print(f"WHODrug示例路径: {demo_whodrug_path}")
    
    # 检查路径是否存在
    if Path(demo_meddra_path).exists():
        print("V MedDRA路径存在")
        meddra_importer = MedDRAImporter(demo_meddra_path)
        meddra_configs = meddra_importer.scan_meddra_directories()
        print(f"找到 {len(meddra_configs)} 个MedDRA配置")
        for config in meddra_configs:
            print(f"  - {config['table_name']}: {config['description']}")
    else:
        print("X MedDRA路径不存在")
    
    if Path(demo_whodrug_path).exists():
        print("V WHODrug路径存在")
        whodrug_importer = WHODrugImporter(demo_whodrug_path)
        whodrug_configs = whodrug_importer.scan_whodrug_directories()
        print(f"找到 {len(whodrug_configs)} 个WHODrug配置")
        for config in whodrug_configs:
            print(f"  - {config['table_name']}: {config['description']}")
    else:
        print("X WHODrug路径不存在")


def create_sample_data_structure():
    """创建示例数据目录结构"""
    print("\n=== 创建示例数据目录结构 ===")
    
    sample_dir = Path('sample_data')
    sample_dir.mkdir(exist_ok=True)
    
    # 创建示例文件说明
    readme_content = """# MedDRA和WHODrug数据目录说明

这个工具支持自动识别MedDRA和WHODrug的标准目录结构并批量导入数据。

## MedDRA 目录结构
```
MedDRA根目录/
├── meddra_18_0_chinese/
│   └── ascii-xxx/
│       ├── hlgt.asc
│       ├── hlt.asc
│       ├── llt.asc
│       ├── pt.asc
│       └── soc.asc
├── meddra_18_0_english/
│   └── MedAscii/
│       ├── hlgt.asc
│       ├── hlt.asc
│       ├── llt.asc
│       ├── pt.asc
│       └── soc.asc
└── meddra_21_0_chinese/
    └── ascii-xxx/
        └── ...
```

## WHODrug 目录结构
```
WHODrug根目录/
├── WHODrug Global 2023 Mar 1/
│   ├── WHODrug B3_C3-format/
│   │   └── whodrug_global_b3_xxx/
│   │       ├── INA.txt
│   │       └── DD.txt
│   └── WHODrug Global Chinese txt_mar_1_2024/
│       └── whodrug_global_b3_xxx/
│           ├── INA.txt
│           └── DD.txt
└── WHODrug Global 2023 Sep 1/
    └── ...
```

## 文件格式说明

### MedDRA文件格式
- 使用$作为分隔符
- 第一部分为代码，第二部分为名称
- 每个文件对应不同的术语级别

### WHODrug文件格式
- 使用空格作为分隔符
- 第一部分为代码，其余部分作为名称
- INA.txt和DD.txt包含不同类型的药物信息

## 使用方法
1. 确保数据目录符合上述结构
2. 运行: python batch_import_example.py
3. 按提示输入MedDRA和WHODrug的根目录路径
4. 工具会自动扫描并导入所有符合规范的数据

## 命令行选项
- `python batch_import_example.py` - 交互式导入
- `python batch_import_example.py demo` - 演示模式（扫描示例路径）
- `python batch_import_example.py create-structure` - 创建目录结构说明
"""
    
    readme_path = sample_dir / 'README.md'
    with open(readme_path, 'w', encoding='utf-8') as f:
        f.write(readme_content)
    
    print(f"V 创建目录: {sample_dir}")
    print(f"V 创建说明: {readme_path}")
    print("\n目录结构说明已创建，请查看 sample_data/README.md 了解详情")


if __name__ == '__main__':
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        if command == 'create-structure':
            create_sample_data_structure()
        elif command == 'demo':
            demo_mode()
        elif command == 'multithread' or command == '-mt':
            # 多线程模式
            max_workers = 4
            if len(sys.argv) > 2:
                try:
                    max_workers = int(sys.argv[2])
                    if max_workers < 1 or max_workers > 16:
                        print("线程数应在1-16之间，使用默认值4")
                        max_workers = 4
                except ValueError:
                    print("无效的线程数，使用默认值4")
                    max_workers = 4
            main_multithread(max_workers)
        elif command == 'singlethread' or command == '-st':
            # 单线程模式
            main()
        elif command == '--help' or command == '-h':
            print("""
MedDRA和WHODrug数据库批量导入工具

用法:
  python batch_import_example.py                     交互式导入模式（自动选择模式）
  python batch_import_example.py singlethread        单线程导入模式
  python batch_import_example.py multithread [线程数] 多线程导入模式（默认4线程）
  python batch_import_example.py demo                演示模式（扫描示例路径）
  python batch_import_example.py create-structure    创建目录结构说明
  python batch_import_example.py --help              显示此帮助信息

多线程选项:
  -st, singlethread     使用单线程模式（兼容性最好）
  -mt, multithread [N]  使用N个线程并行导入（默认4个，推荐2-8个）

说明:
  此工具自动识别MedDRA和WHODrug的标准目录结构，
  解析版本信息和语言类型，批量导入数据到数据库中。

  MedDRA目录格式: meddra_版本_语言 (如 meddra_18_0_chinese)
  WHODrug目录格式: WHODrug Global 版本 (如 WHODrug Global 2023 Mar 1)

性能提升:
  多线程模式可以显著提升导入速度，特别是在处理大量文件时。
  建议根据CPU核心数选择合适的线程数。

增量导入:
  支持增量导入模式，只导入数据库中不存在的表，避免重复导入。
  可选择：1) 重新初始化数据库（清空所有数据）
          2) 增量导入（只导入不存在的表，默认）

示例:
  python batch_import_example.py                      # 默认模式（增量导入）
  python batch_import_example.py multithread 6       # 使用6线程增量导入
  python batch_import_example.py singlethread        # 单线程增量导入
  
  运行后选择模式：
  1. 重新初始化数据库（清空所有数据）- 完全重新导入
  2. 增量导入（只导入不存在的表）- 智能跳过已存在的表
  
  然后按提示输入：
  MedDRA根目录路径: \\\\192.168.123.22\\z\\Public\\DataCenter\\Coding\\MedDRA
  WHODrug根目录路径: \\\\192.168.123.22\\z\\Public\\DataCenter\\Coding\\WHODrug
            """)
        else:
            print(f"未知命令: {command}")
            print("使用 --help 查看帮助信息")
    else:
        # 默认使用多线程模式（自动检测）
        print("提示: 默认使用多线程模式以提升性能")
        print("      如需单线程模式，请使用: python batch_import_example.py singlethread")
        print("      如需自定义线程数，请使用: python batch_import_example.py multithread <线程数>\n")
        
        # 自动检测合适的线程数
        import os
        cpu_count = os.cpu_count() or 4
        max_workers = min(max(cpu_count // 2, 2), 8)  # 使用CPU核心数的一半，最少2个，最多8个
        print(f"自动检测到CPU核心数: {cpu_count}，使用 {max_workers} 个线程\n")
        
        main_multithread(max_workers)
