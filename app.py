from flask import Flask, render_template, request, jsonify
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
                INSERT OR REPLACE INTO mapping_configs 
                (path_hash, path, mode, translation_direction, name, configs, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (path_hash, path, mode, translation_direction, name, configs_json, datetime.now()))
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
            ''', (path_hash, path, mode, translation_direction, meddra_version, whodrug_version, ig_version, meddra_config_json, whodrug_config_json, datetime.now()))
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
            ''', (path_hash, path, translation_direction, configs_json, datetime.now()))
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
            cursor.execute('SELECT name, description, record_count, version FROM meddra_tables ORDER BY name')
            meddra_tables = [{'name': row[0], 'description': row[1], 'record_count': row[2], 'version': row[3]} 
                           for row in cursor.fetchall()]
            
            # 获取WhoDrug表
            cursor.execute('SELECT name, description, record_count, version FROM whodrug_tables ORDER BY name')
            whodrug_tables = [{'name': row[0], 'description': row[1], 'record_count': row[2], 'version': row[3]} 
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

class SASDataProcessor:
    def __init__(self):
        self.datasets = {}
        self.merged_datasets = {}
        self.hide_supp_in_preview = False
        self.translation_direction = 'zh_to_en'  # 默认中译英
        self.current_path = None  # 当前数据路径

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
                # 构建预览视图并执行SUPP数据合并
                self._build_preview_views()
                self._auto_merge_all_supp_data()  # 执行SUPP数据合并
            
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
                
                # 自动将所有SUPP数据集中的QNAM转置并合并到对应的主数据集中
                self._auto_merge_all_supp_data()

            return True, "变量合并成功"
        except Exception as e:
            # 将异常同时打印到控制台，便于调试
            print('merge_variables error:', e)
            return False, f"变量合并失败: {str(e)}"

    def _auto_merge_all_supp_data(self) -> None:
        """自动将所有SUPP数据集中的QNAM转置并合并到对应的主数据集中"""
        try:
            # 获取所有SUPP数据集
            supp_datasets = {name: data for name, data in self.datasets.items() if name.upper().startswith('SUPP')}
            
            for supp_name, supp_entry in supp_datasets.items():
                supp_df = supp_entry.get('raw_data', supp_entry['data']).copy()
                
                # 检查必要的列
                if 'QNAM' not in supp_df.columns or 'QVAL' not in supp_df.columns:
                    continue
                    
                # 按RDOMAIN分组处理
                if 'RDOMAIN' in supp_df.columns:
                    for rdomain in supp_df['RDOMAIN'].dropna().unique():
                        rdomain_str = str(rdomain).upper()
                        
                        # 找到对应的主数据集
                        target_dataset = None
                        for ds_name in self.datasets.keys():
                            if ds_name.upper() == rdomain_str and not ds_name.upper().startswith('SUPP'):
                                target_dataset = ds_name
                                break
                        
                        if not target_dataset:
                            continue
                            
                        # 获取该RDOMAIN的SUPP数据
                        rdomain_supp = supp_df[supp_df['RDOMAIN'].astype(str).str.upper() == rdomain_str].copy()
                        
                        if rdomain_supp.empty:
                            continue
                            
                        # 调用现有的SUPP合并方法
                        self._merge_supp_data(supp_name, target_dataset, rdomain_supp)
                        
        except Exception as e:
            print(f'_auto_merge_all_supp_data error: {e}')
    
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
        conn = sqlite3.connect(db_manager.db_path)
        cursor = conn.cursor()
        
        # 直接从meddra_merged表中获取version字段的非重复值
        cursor.execute('SELECT DISTINCT version FROM meddra_merged WHERE version IS NOT NULL ORDER BY version')
        versions = cursor.fetchall()
        
        meddra_versions = []
        for version_row in versions:
            version = version_row[0]
            if version:
                # 获取该版本的记录数
                cursor.execute('SELECT COUNT(*) FROM meddra_merged WHERE version = ?', (version,))
                count = cursor.fetchone()[0]
                
                meddra_versions.append({
                    'version': version,
                    'name': f'MedDRA {version}',
                    'description': f'MedDRA版本 {version}',
                    'record_count': count
                })
        
        conn.close()
        return jsonify({'versions': meddra_versions})
    except Exception as e:
        return jsonify({'versions': [], 'error': str(e)})

@app.route('/api/get_whodrug_versions', methods=['GET'])
def get_whodrug_versions():
    """获取WHODrug版本列表"""
    try:
        conn = sqlite3.connect(db_manager.db_path)
        cursor = conn.cursor()
        
        # 直接从whodrug_merged表中获取version字段的非重复值
        cursor.execute('SELECT DISTINCT version FROM whodrug_merged WHERE version IS NOT NULL ORDER BY version')
        versions = cursor.fetchall()
        
        whodrug_versions = []
        for version_row in versions:
            version = version_row[0]
            if version:
                # 获取该版本的记录数
                cursor.execute('SELECT COUNT(*) FROM whodrug_merged WHERE version = ?', (version,))
                count = cursor.fetchone()[0]
                
                whodrug_versions.append({
                    'version': version,
                    'name': f'WHODrug {version}',
                    'description': f'WHODrug版本 {version}',
                    'record_count': count
                })
        
        conn.close()
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
        
        # 获取翻译库配置
        db_manager = DatabaseManager()
        config = db_manager.get_translation_library_config(path)
        
        if not config:
            return jsonify({'success': False, 'message': '未找到翻译库配置'}), 400
        
        meddra_config = config.get('meddra_config', [])
        whodrug_config = config.get('whodrug_config', [])
        meddra_version = config.get('meddra_version')
        whodrug_version = config.get('whodrug_version')
        
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
        # 检查是否已经加载了数据，如果没有数据或路径不匹配才重新读取
        if not processor.datasets or processor.current_path != path:
            print(f'processor.datasets为空或路径不匹配，开始读取SAS文件: {path}')
            success, message = processor.read_sas_files(path, mode='SDTM')  # 使用SDTM模式以启用SUPP数据合并
            if not success:
                return jsonify({'success': False, 'message': f'读取SAS文件失败: {message}'}), 500
            processor.current_path = path  # 记录当前路径
            print(f'SAS文件读取完成，共加载 {len(processor.datasets)} 个数据集')
        else:
            print(f'使用已存在的processor数据，共 {len(processor.datasets)} 个数据集（包含合并后的数据）')
            # 路径匹配且有数据，直接使用现有数据（包含合并后的数据）
        
        coded_items = []
        # 用于去重的集合，存储 (数据集名称, 变量名称, 原始值) 的组合
        processed_items = set()
        # 统一收集所有未匹配的项目，用于批量AI翻译
        unmatched_items = {
            'meddra': [],  # MedDRA未匹配项目
            'whodrug': []  # WHODrug未匹配项目
        }
        
        print(f'开始处理编码清单，MedDRA配置项: {len(meddra_config)}, WHODrug配置项: {len(whodrug_config)}')
        
        # 处理MedDRA配置的变量
        for idx, meddra_item in enumerate(meddra_config):
            print(f'处理MedDRA配置项 {idx+1}/{len(meddra_config)}: {meddra_item.get("name_column")}')
            if meddra_item.get('name_column'):
                variable_name = meddra_item['name_column']
                code_variable = meddra_item.get('code_column')
                
                # 从合并数据中获取该变量的所有值
                for dataset_name, dataset_info in processor.datasets.items():
                    df = dataset_info['data']
                    if variable_name in df.columns:
                        unique_values = df[variable_name].dropna().unique()
                        # 不再限制处理的唯一值数量，处理所有数据
                        print(f'  数据集 {dataset_name} 中变量 {variable_name} 有 {len(unique_values)} 个唯一值')
                        
                        print(f'    处理 {len(unique_values)} 个唯一值...')
                        
                        # 批量查询翻译数据以提高性能
                        translation_dict = {}
                        conn = sqlite3.connect('translation_db.sqlite')
                        try:
                            cursor = conn.cursor()
                            if code_variable and code_variable in df.columns:
                                # 有code列的情况，直接使用code列的值进行匹配
                                # 获取code列的唯一值
                                code_unique_values = df[code_variable].dropna().unique()
                                
                                if len(code_unique_values) > 0:
                                    # 处理code值格式，将浮点数转换为整数字符串
                                    processed_codes = []
                                    for code in code_unique_values:
                                        if pd.notna(code):
                                            try:
                                                if isinstance(code, float) and code.is_integer():
                                                    processed_codes.append(str(int(code)))
                                                else:
                                                    processed_codes.append(str(code).strip())
                                            except:
                                                processed_codes.append(str(code).strip())
                                    
                                    placeholders = ','.join(['?'] * len(processed_codes))
                                    meddra_query = f"SELECT code, name_cn, name_en FROM meddra_merged WHERE code IN ({placeholders}) AND version = ?"
                                    cursor.execute(meddra_query, processed_codes + [meddra_version])
                                    
                                    # 建立code到翻译的映射
                                    code_to_translation = {}
                                    for row in cursor.fetchall():
                                        code, name_cn, name_en = row
                                        if translation_direction == 'zh_to_en':
                                            code_to_translation[str(code).strip()] = name_en
                                        else:
                                            code_to_translation[str(code).strip()] = name_cn
                                    
                                    # 为每个name值找到对应的code值，然后获取翻译
                                for value in unique_values:
                                    # 找到该name值对应的code值
                                    matching_rows = df[df[variable_name] == value]
                                    if len(matching_rows) > 0:
                                        raw_code = matching_rows[code_variable].iloc[0]
                                        # 处理数字类型的code值，去除.0后缀
                                        if pd.notna(raw_code):
                                            try:
                                                if isinstance(raw_code, float) and raw_code.is_integer():
                                                    code_value = str(int(raw_code))
                                                else:
                                                    code_value = str(raw_code).strip()
                                            except:
                                                code_value = str(raw_code).strip()
                                        else:
                                            code_value = ''
                                        
                                        if code_value in code_to_translation:
                                            translation_dict[str(value)] = code_to_translation[code_value]
                            else:
                                # 只有name列的情况，批量查询name值
                                if translation_direction == 'zh_to_en':
                                    placeholders = ','.join(['?'] * len(unique_values))
                                    meddra_query = f"SELECT name_cn, name_en FROM meddra_merged WHERE name_cn IN ({placeholders}) AND version = ?"
                                    cursor.execute(meddra_query, [str(v) for v in unique_values] + [meddra_version])
                                    for row in cursor.fetchall():
                                        translation_dict[row[0]] = row[1]
                                else:
                                    placeholders = ','.join(['?'] * len(unique_values))
                                    meddra_query = f"SELECT name_en, name_cn FROM meddra_merged WHERE name_en IN ({placeholders}) AND version = ?"
                                    cursor.execute(meddra_query, [str(v) for v in unique_values] + [meddra_version])
                                    for row in cursor.fetchall():
                                        translation_dict[row[0]] = row[1]
                        finally:
                            conn.close()
                        
                        # 处理每个唯一值
                        for idx, value in enumerate(unique_values):
                            if idx % 20 == 0:  # 每20个值显示一次进度
                                print(f'      进度: {idx+1}/{len(unique_values)}')
                            
                            # 检查是否需要翻译
                            if not should_translate_value(value, translation_direction):
                                continue
                            
                            # 获取对应的code值（如果有code列配置）
                            code_value = ''
                            if code_variable and code_variable in df.columns:
                                matching_rows = df[df[variable_name] == value]
                                if len(matching_rows) > 0:
                                    raw_code = matching_rows[code_variable].iloc[0]
                                    # 处理数字类型的code值，去除.0后缀
                                    if pd.notna(raw_code):
                                        try:
                                            # 如果是浮点数且为整数值，转换为整数字符串
                                            if isinstance(raw_code, float) and raw_code.is_integer():
                                                code_value = str(int(raw_code))
                                            else:
                                                code_value = str(raw_code).strip()
                                        except:
                                            code_value = str(raw_code).strip()
                            
                            # 去重检查：按照数据集名称、变量名称、原始值和code值进行去重
                            item_key = (dataset_name, variable_name, str(value), code_value)
                            if item_key in processed_items:
                                continue  # 跳过重复项
                            processed_items.add(item_key)
                            
                            # 优先使用code值进行匹配，如果没有code值则使用name值匹配
                            translated_value = ''
                            if code_value and code_variable:
                                # 有code值时，优先使用code值匹配
                                translated_value = code_to_translation.get(code_value, '')
                            
                            if not translated_value:
                                # code值匹配失败或没有code值时，使用name值匹配
                                translated_value = translation_dict.get(str(value), '')
                            
                            if translated_value:
                                translation_source = f'meddra_merged_{meddra_version}'
                                needs_confirmation = 'N'
                            else:
                                # 收集未匹配项目，稍后统一AI翻译
                                if is_ai_translation_eligible(value):
                                    unmatched_items['meddra'].append({
                                        'value': str(value),
                                        'dataset': dataset_name,
                                        'variable': variable_name,
                                        'code': code_value,
                                        'index': len(coded_items)
                                    })
                                translated_value = ''  # 暂时为空，稍后AI翻译
                                translation_source = 'AI'
                                needs_confirmation = 'Y'
                            
                            coded_items.append({
                                'dataset': dataset_name,
                                'variable': variable_name,
                                'value': str(value),
                                'code': code_value,
                                'translated_value': translated_value,
                                'translation_source': translation_source,
                                'needs_confirmation': needs_confirmation
                            })
        
        # 处理WHODrug配置的变量
        for idx, whodrug_item in enumerate(whodrug_config):
            print(f'处理WHODrug配置项 {idx+1}/{len(whodrug_config)}: {whodrug_item.get("name_column")}')
            if whodrug_item.get('name_column'):
                variable_name = whodrug_item['name_column']
                code_variable = whodrug_item.get('code_column')
                
                # 从合并数据中获取该变量的所有值
                for dataset_name, dataset_info in processor.datasets.items():
                    df = dataset_info['data']
                    if variable_name in df.columns:
                        unique_values = df[variable_name].dropna().unique()
                        # 不再限制处理的唯一值数量，处理所有数据
                        print(f'  数据集 {dataset_name} 中变量 {variable_name} 有 {len(unique_values)} 个唯一值')
                        
                        print(f'    处理 {len(unique_values)} 个唯一值...')
                        
                        # 批量查询翻译数据以提高性能
                        translation_dict = {}
                        conn = sqlite3.connect('translation_db.sqlite')
                        try:
                            cursor = conn.cursor()
                            if code_variable and code_variable in df.columns:
                                # 有code列的情况，直接使用code列的值进行匹配
                                # 获取code列的唯一值
                                code_unique_values = df[code_variable].dropna().unique()
                                
                                if len(code_unique_values) > 0:
                                    # 处理code值格式，将浮点数转换为整数字符串
                                    processed_codes = []
                                    for code in code_unique_values:
                                        if pd.notna(code):
                                            try:
                                                if isinstance(code, float) and code.is_integer():
                                                    processed_codes.append(str(int(code)))
                                                else:
                                                    processed_codes.append(str(code).strip())
                                            except:
                                                processed_codes.append(str(code).strip())
                                    
                                    placeholders = ','.join(['?'] * len(processed_codes))
                                    whodrug_query = f"SELECT code, name_cn, name_en FROM whodrug_merged WHERE code IN ({placeholders}) AND version = ?"
                                    cursor.execute(whodrug_query, processed_codes + [whodrug_version])
                                    
                                    # 建立code到翻译的映射
                                    code_to_translation = {}
                                    for row in cursor.fetchall():
                                        code, name_cn, name_en = row
                                        if translation_direction == 'zh_to_en':
                                            code_to_translation[str(code).strip()] = name_en
                                        else:
                                            code_to_translation[str(code).strip()] = name_cn
                                    
                                    # 为每个name值找到对应的code值，然后获取翻译
                                for value in unique_values:
                                    # 找到该name值对应的code值
                                    matching_rows = df[df[variable_name] == value]
                                    if len(matching_rows) > 0:
                                        code_value = str(matching_rows[code_variable].iloc[0]).strip()
                                        if code_value in code_to_translation:
                                            translation_dict[str(value)] = code_to_translation[code_value]
                            else:
                                # 只有name列的情况，批量查询name值
                                if translation_direction == 'zh_to_en':
                                    placeholders = ','.join(['?'] * len(unique_values))
                                    whodrug_query = f"SELECT name_cn, name_en FROM whodrug_merged WHERE name_cn IN ({placeholders}) AND version = ?"
                                    cursor.execute(whodrug_query, [str(v) for v in unique_values] + [whodrug_version])
                                    for row in cursor.fetchall():
                                        translation_dict[row[0]] = row[1]
                                else:
                                    placeholders = ','.join(['?'] * len(unique_values))
                                    whodrug_query = f"SELECT name_en, name_cn FROM whodrug_merged WHERE name_en IN ({placeholders}) AND version = ?"
                                    cursor.execute(whodrug_query, [str(v) for v in unique_values] + [whodrug_version])
                                    for row in cursor.fetchall():
                                        translation_dict[row[0]] = row[1]
                        finally:
                            conn.close()
                        
                        # 处理每个唯一值
                        for idx, value in enumerate(unique_values):
                            if idx % 20 == 0:  # 每20个值显示一次进度
                                print(f'      进度: {idx+1}/{len(unique_values)}')
                            
                            # 检查是否需要翻译
                            if not should_translate_value(value, translation_direction):
                                continue
                            
                            # 获取对应的code值（如果有code列配置）
                            code_value = ''
                            if code_variable and code_variable in df.columns:
                                matching_rows = df[df[variable_name] == value]
                                if len(matching_rows) > 0:
                                    raw_code = matching_rows[code_variable].iloc[0]
                                    # 处理数字类型的code值，去除.0后缀
                                    if pd.notna(raw_code):
                                        try:
                                            # 如果是浮点数且为整数值，转换为整数字符串
                                            if isinstance(raw_code, float) and raw_code.is_integer():
                                                code_value = str(int(raw_code))
                                            else:
                                                code_value = str(raw_code).strip()
                                        except:
                                            code_value = str(raw_code).strip()
                            
                            # 去重检查：按照数据集名称、变量名称、原始值和code值进行去重
                            item_key = (dataset_name, variable_name, str(value), code_value)
                            if item_key in processed_items:
                                continue  # 跳过重复项
                            processed_items.add(item_key)
                            
                            # 优先使用code值进行匹配，如果没有code值则使用name值匹配
                            translated_value = ''
                            if code_value and code_variable:
                                # 有code值时，优先使用code值匹配
                                translated_value = code_to_translation.get(code_value, '')
                            
                            if not translated_value:
                                # code值匹配失败或没有code值时，使用name值匹配
                                translated_value = translation_dict.get(str(value), '')
                            
                            if translated_value:
                                translation_source = f'whodrug_merged_{whodrug_version}'
                                needs_confirmation = 'N'
                            else:
                                # 收集未匹配项目，稍后统一AI翻译
                                if is_ai_translation_eligible(value):
                                    unmatched_items['whodrug'].append({
                                        'value': str(value),
                                        'dataset': dataset_name,
                                        'variable': variable_name,
                                        'code': code_value,
                                        'index': len(coded_items)
                                    })
                                translated_value = ''  # 暂时为空，稍后AI翻译
                                translation_source = 'AI'
                                needs_confirmation = 'Y'
                            
                            coded_items.append({
                                'dataset': dataset_name,
                                'variable': variable_name,
                                'value': str(value),
                                'code': code_value,
                                'translated_value': translated_value,
                                'translation_source': translation_source,
                                'needs_confirmation': needs_confirmation
                            })
                        
        
        # 统一批量AI翻译所有未匹配项目
        print(f'\n开始统一AI翻译，MedDRA未匹配: {len(unmatched_items["meddra"])}项，WHODrug未匹配: {len(unmatched_items["whodrug"])}项')
        
        # 如果项目太多，只处理前50个并给出提示
        if len(unmatched_items['meddra']) > 50:
            print(f'⚠️  MedDRA未匹配项目过多({len(unmatched_items["meddra"])}项)，只处理前50项')
            unmatched_items['meddra'] = unmatched_items['meddra'][:50]
        
        if len(unmatched_items['whodrug']) > 50:
            print(f'⚠️  WHODrug未匹配项目过多({len(unmatched_items["whodrug"])}项)，只处理前50项')
            unmatched_items['whodrug'] = unmatched_items['whodrug'][:50]
        
        # 处理MedDRA未匹配项目
        if unmatched_items['meddra'] and len(unmatched_items['meddra']) <= 50:
            print(f'批量AI翻译MedDRA项目: {len(unmatched_items["meddra"])}项')
            meddra_values = [item['value'] for item in unmatched_items['meddra']]
            
            # 构建AI翻译提示，包含编码类型和上下文信息
            context_info = f"这些是MedDRA编码变量的值，请进行医学术语翻译。翻译方向：{'英译中' if translation_direction == 'en_to_zh' else '中译英'}"
            
            try:
                # 批量调用AI翻译
                batch_size = 3
                for batch_start in range(0, len(meddra_values), batch_size):
                    batch_end = min(batch_start + batch_size, len(meddra_values))
                    batch_values = meddra_values[batch_start:batch_end]
                    batch_items = unmatched_items['meddra'][batch_start:batch_end]
                    
                    print(f'  AI翻译MedDRA批次: {batch_start+1}-{batch_end}/{len(meddra_values)}')
                    
                    # 构建批量翻译请求
                    batch_text = '\n'.join([f'{i+1}. {value}' for i, value in enumerate(batch_values)])
                    prompt = f"{context_info}\n\n待翻译内容：\n{batch_text}\n\n请按序号返回翻译结果，每行一个。"
                    
                    if translation_direction == 'zh_to_en':
                        ai_result = translation_service.translate_text(prompt, 'zh', 'en', 'medical')
                    else:
                        ai_result = translation_service.translate_text(prompt, 'en', 'zh', 'medical')
                    
                    if ai_result.get('success'):
                        translated_lines = ai_result.get('translated_text', '').strip().split('\n')
                        print(f'    AI翻译成功，返回{len(translated_lines)}行结果')
                        for i, item in enumerate(batch_items):
                            if i < len(translated_lines):
                                # 提取翻译结果（去除序号）
                                translated = translated_lines[i].strip()
                                if '. ' in translated:
                                    translated = translated.split('. ', 1)[1]
                                coded_items[item['index']]['translated_value'] = translated
                                print(f'      更新项目{item["index"]}: "{item["value"]}" -> "{translated}"')
                    else:
                        print(f'    AI翻译失败: {ai_result.get("error", "未知错误")}')
            except Exception as e:
                print(f'MedDRA批量AI翻译失败: {e}')
        
        # 处理WHODrug未匹配项目
        if unmatched_items['whodrug'] and len(unmatched_items['whodrug']) <= 50:
            print(f'批量AI翻译WHODrug项目: {len(unmatched_items["whodrug"])}项')
            whodrug_values = [item['value'] for item in unmatched_items['whodrug']]
            
            # 构建AI翻译提示，包含编码类型和上下文信息
            context_info = f"这些是WHODrug药物编码变量的值，请进行药物术语翻译。翻译方向：{'英译中' if translation_direction == 'en_to_zh' else '中译英'}"
            
            try:
                # 批量调用AI翻译
                batch_size = 3
                for batch_start in range(0, len(whodrug_values), batch_size):
                    batch_end = min(batch_start + batch_size, len(whodrug_values))
                    batch_values = whodrug_values[batch_start:batch_end]
                    batch_items = unmatched_items['whodrug'][batch_start:batch_end]
                    
                    print(f'  AI翻译WHODrug批次: {batch_start+1}-{batch_end}/{len(whodrug_values)}')
                    
                    # 构建批量翻译请求
                    batch_text = '\n'.join([f'{i+1}. {value}' for i, value in enumerate(batch_values)])
                    prompt = f"{context_info}\n\n待翻译内容：\n{batch_text}\n\n请按序号返回翻译结果，每行一个。"
                    
                    if translation_direction == 'zh_to_en':
                        ai_result = translation_service.translate_text(prompt, 'zh', 'en', 'medical')
                    else:
                        ai_result = translation_service.translate_text(prompt, 'en', 'zh', 'medical')
                    
                    if ai_result.get('success'):
                        translated_lines = ai_result.get('translated_text', '').strip().split('\n')
                        print(f'    AI翻译成功，返回{len(translated_lines)}行结果')
                        for i, item in enumerate(batch_items):
                            if i < len(translated_lines):
                                # 提取翻译结果（去除序号）
                                translated = translated_lines[i].strip()
                                if '. ' in translated:
                                    translated = translated.split('. ', 1)[1]
                                coded_items[item['index']]['translated_value'] = translated
                                print(f'      更新项目{item["index"]}: "{item["value"]}" -> "{translated}"')
                    else:
                        print(f'    AI翻译失败: {ai_result.get("error", "未知错误")}')
            except Exception as e:
                print(f'WHODrug批量AI翻译失败: {e}')
        
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
                item['needs_confirmation'] = 'Y'
                item['highlight'] = True  # 添加高亮标记
            else:
                item['highlight'] = False
        
        # 对编码清单进行排序：按数据集、变量名、原始值、code列排序
        coded_items.sort(key=lambda x: (
            x.get('dataset', ''),
            x.get('variable', ''),
            x.get('value', ''),
            x.get('code', '')
        ))
        
        # 不再限制返回的项目数量，返回所有项目
        result = {
            'success': True,
            'message': f'编码清单生成成功，共{len(coded_items)}项',
            'data': {
                'translation_direction': translation_direction,
                'meddra_version': meddra_version,
                'whodrug_version': whodrug_version,
                'coded_items': coded_items,
                'total_count': len(coded_items),
                'displayed_count': len(coded_items),
                'is_limited': False
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
        
        # 分页参数
        page = data.get('page', 1)  # 页码，默认第1页
        page_size = data.get('page_size', 100)  # 每页大小，默认100条
        
        # 验证分页参数
        if page < 1:
            page = 1
        if page_size < 1 or page_size > 1000:  # 限制每页最大1000条
            page_size = 100
        
        # 获取翻译库配置
        db_manager = DatabaseManager()
        translation_config = db_manager.get_translation_library_config(path)
        
        if not translation_config:
            return jsonify({'success': False, 'message': '未找到翻译库配置，请先保存配置'}), 400
        
        # 使用全局processor实例获取合并后的数据
        # 确保processor已经加载了数据
        if not processor.datasets:
            processor.read_sas_files(path)
        
        # 获取MedDRA和WHODrug配置表中的变量名
        meddra_config = translation_config.get('meddra_config', [])
        whodrug_config = translation_config.get('whodrug_config', [])
        
        # 收集编码变量名
        coded_variables = set()
        for item in meddra_config:
            if item.get('name_column'):
                coded_variables.add(item['name_column'])
        for item in whodrug_config:
            if item.get('name_column'):
                coded_variables.add(item['name_column'])
        
        # 生成非编码清单
        uncoded_items = []
        
        # 遍历所有数据集和变量，找出不在编码配置中的变量
        for dataset_name, dataset_info in processor.datasets.items():
            df = dataset_info['data']
            
            for column in df.columns:
                if column not in coded_variables:
                    # 获取该变量的唯一值
                    unique_values = df[column].dropna().unique()
                    
                    for value in unique_values:
                        translated_value = ''
                        translation_source = 'metadata'  # 使用metadata进行翻译
                        needs_confirmation = 'Y'  # 非编码项需要确认
                        
                        # 从metadata表匹配翻译
                        conn = sqlite3.connect('translation_db.sqlite')
                        cursor = conn.cursor()
                        
                        try:
                            # 根据翻译方向查询metadata表
                            if translation_direction == 'zh_to_en':
                                # 中译英：用原始值匹配__TEST_CN，获取__TEST_EN
                                cursor.execute("""
                                    SELECT __TEST_EN FROM metadata 
                                    WHERE __TEST_CN = ? AND __TEST_EN IS NOT NULL AND __TEST_EN != ''
                                """, (str(value),))
                            else:
                                # 英译中：用原始值匹配__TEST_EN，获取__TEST_CN
                                cursor.execute("""
                                    SELECT __TEST_CN FROM metadata 
                                    WHERE __TEST_EN = ? AND __TEST_CN IS NOT NULL AND __TEST_CN != ''
                                """, (str(value),))
                            
                            metadata_results = cursor.fetchall()
                            
                            if metadata_results:
                                # 检查是否有多个翻译结果
                                unique_translations = list(set([result[0] for result in metadata_results]))
                                
                                if len(unique_translations) == 1:
                                    translated_value = unique_translations[0]
                                    needs_confirmation = 'N'  # metadata匹配不需要确认
                                elif len(unique_translations) > 1:
                                    # 多个翻译结果，需要高亮确认
                                    translated_value = ' | '.join(unique_translations)
                                    needs_confirmation = 'Y'  # 多个结果需要确认
                                    translation_source = 'metadata_multiple'
                        
                        except Exception as metadata_error:
                            print(f"Metadata查询错误: {metadata_error}")
                        finally:
                            conn.close()
                        
                        # 如果metadata无法匹配，使用AI翻译
                        if not translated_value:
                            if translation_direction == 'en_to_zh':
                                ai_result = translation_service.translate_text(
                                    str(value), 'en', 'zh', 'medical'
                                )
                            elif translation_direction == 'zh_to_en':
                                ai_result = translation_service.translate_text(
                                    str(value), 'zh', 'en', 'medical'
                                )
                            else:
                                ai_result = {'success': False}
                            
                            if ai_result.get('success'):
                                translated_value = ai_result.get('translated_text', '')
                                translation_source = 'AI'
                                needs_confirmation = 'Y'  # AI翻译需要确认
                        
                        uncoded_items.append({
                            'dataset': dataset_name,
                            'variable': column,
                            'value': str(value),
                            'translated_value': translated_value,
                            'translation_source': translation_source,
                            'needs_confirmation': needs_confirmation
                        })
        
        # 检测重复值并标记需要确认的项目
        value_translations = {}
        for item in uncoded_items:
            key = f"{item['variable']}|{item['value']}"
            if key not in value_translations:
                value_translations[key] = set()
            if item['translated_value']:
                value_translations[key].add(item['translated_value'])
        
        # 标记有多个翻译结果的项目
        for item in uncoded_items:
            key = f"{item['variable']}|{item['value']}"
            if len(value_translations.get(key, set())) > 1:
                item['needs_confirmation'] = 'Y'
                item['highlight'] = True  # 添加高亮标记
            else:
                item['highlight'] = False
        
        # 对非编码清单进行排序：按数据集、变量名、原始值排序
        uncoded_items.sort(key=lambda x: (x['dataset'], x['variable'], x['value']))
        
        # 计算分页
        total_count = len(uncoded_items)
        start_index = (page - 1) * page_size
        end_index = start_index + page_size
        paginated_items = uncoded_items[start_index:end_index]
        
        # 计算总页数
        total_pages = (total_count + page_size - 1) // page_size
        
        result = {
            'success': True,
            'message': '非编码清单生成成功',
            'data': {
                'translation_direction': translation_direction,
                'uncoded_items': paginated_items,
                'pagination': {
                    'current_page': page,
                    'page_size': page_size,
                    'total_count': total_count,
                    'total_pages': total_pages,
                    'has_next': page < total_pages,
                    'has_prev': page > 1
                }
            }
        }
        
        return jsonify(result)
        
    except Exception as e:
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
        
        # 生成数据集标签清单
        dataset_labels = []
        
        # 遍历所有数据集，生成标签信息
        for dataset_name, dataset_info in processor.datasets.items():
            # 获取数据集的基本信息
            original_label = dataset_info.get('label', '')
            translated_label = ''
            translation_source = f'sdtm2dataset_{ig_version}'
            needs_confirmation = 'Y'
            
            # TODO: 实现从sdtm2dataset表的匹配逻辑
            
            # 如果sdtm2dataset无法匹配，使用AI翻译
            if not translated_label and original_label and translation_direction == 'en_to_zh':
                ai_result = translation_service.translate_text(
                    original_label, 'en', 'zh', 'medical'
                )
                if ai_result.get('success'):
                    translated_label = ai_result.get('translated_text', '')
                    translation_source = 'AI'
                    needs_confirmation = 'Y'  # AI翻译需要确认
            
            dataset_labels.append({
                'dataset': dataset_name,
                'original_label': original_label,
                'translated_label': translated_label,
                'translation_source': translation_source,
                'needs_confirmation': needs_confirmation
            })
        
        result = {
            'success': True,
            'message': '数据集Label生成成功',
            'data': {
                'translation_direction': translation_direction,
                'ig_version': ig_version,
                'dataset_labels': dataset_labels,
                'total_count': len(dataset_labels)
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
        
        # 遍历所有数据集和变量，生成变量标签信息
        for dataset_name, dataset_info in processor.datasets.items():
            df = dataset_info['data']
            
            for column in df.columns:
                # 获取变量的基本信息
                original_label = ''
                # 尝试从数据集元数据中获取变量标签
                if hasattr(df, 'attrs') and 'column_labels' in df.attrs:
                    original_label = df.attrs['column_labels'].get(column, '')
                
                translated_label = ''
                translation_source = f'sdtm2variable_{ig_version}'
                needs_confirmation = 'Y'
                
                # 检查是否为SUPP变量
                if column.startswith('QNAM') or column.startswith('QVAL'):
                    translation_source = f'supp_qnamlist_{ig_version}'
                
                # TODO: 实现从sdtm2variable和supp_qnamlist表的匹配逻辑
                
                # 如果翻译库无法匹配，使用AI翻译
                if not translated_label and original_label and translation_direction == 'en_to_zh':
                    ai_result = translation_service.translate_text(
                        original_label, 'en', 'zh', 'medical'
                    )
                    if ai_result.get('success'):
                        translated_label = ai_result.get('translated_text', '')
                        translation_source = 'AI'
                        needs_confirmation = 'Y'  # AI翻译需要确认
                
                variable_labels.append({
                    'dataset': dataset_name,
                    'variable': column,
                    'original_label': original_label,
                    'translated_label': translated_label,
                    'translation_source': translation_source,
                    'needs_confirmation': needs_confirmation
                })
        
        result = {
            'success': True,
            'message': '变量Label生成成功',
            'data': {
                'translation_direction': translation_direction,
                'ig_version': ig_version,
                'variable_labels': variable_labels,
                'total_count': len(variable_labels)
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
        self.max_retries = 3
        self.timeout = 60  # 增加超时时间到60秒
        
        # 性能统计
        self.stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'total_time': 0,
            'retry_count': 0
        }
    
    def get_stats(self):
        """获取翻译统计信息"""
        if self.stats['total_requests'] > 0:
            success_rate = (self.stats['successful_requests'] / self.stats['total_requests']) * 100
            avg_time = self.stats['total_time'] / self.stats['total_requests']
        else:
            success_rate = 0
            avg_time = 0
        
        return {
            'total_requests': self.stats['total_requests'],
            'successful_requests': self.stats['successful_requests'],
            'failed_requests': self.stats['failed_requests'],
            'success_rate': f'{success_rate:.1f}%',
            'average_time': f'{avg_time:.2f}s',
            'retry_count': self.stats['retry_count']
        }
    
    def translate_text(self, text, source_lang='en', target_lang='zh', context='medical'):
        """翻译文本，带重试机制和性能统计"""
        import time
        start_time = time.time()
        self.stats['total_requests'] += 1
        
        if not self.api_key:
            self.stats['failed_requests'] += 1
            return {
                'success': False,
                'error': 'DeepSeek API密钥未配置',
                'original_text': text
            }
        
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
        
        # 重试机制
        for attempt in range(self.max_retries):
            try:
                if attempt > 0:
                    self.stats['retry_count'] += 1
                    print(f"AI翻译重试 {attempt}/{self.max_retries}: {text[:50]}...")
                
                response = requests.post(self.base_url, headers=self.headers, json=payload, timeout=self.timeout)
                response.raise_for_status()
                
                result = response.json()
                translated_text = result['choices'][0]['message']['content'].strip()
                
                # 记录成功统计
                elapsed_time = time.time() - start_time
                self.stats['successful_requests'] += 1
                self.stats['total_time'] += elapsed_time
                
                if attempt > 0:
                    print(f"AI翻译重试成功: {text[:30]}... -> {translated_text[:30]}... (耗时: {elapsed_time:.2f}s)")
                
                return {
                    'success': True,
                    'translated_text': translated_text,
                    'original_text': text,
                    'source_lang': source_lang,
                    'target_lang': target_lang,
                    'elapsed_time': elapsed_time
                }
                
            except requests.exceptions.Timeout:
                error_msg = f'API请求超时 (尝试 {attempt + 1}/{self.max_retries})'
                print(f"AI翻译超时: {error_msg} - 文本: {text[:50]}...")
                if attempt == self.max_retries - 1:
                    self.stats['failed_requests'] += 1
                    elapsed_time = time.time() - start_time
                    self.stats['total_time'] += elapsed_time
                    return {
                        'success': False,
                        'error': error_msg,
                        'original_text': text,
                        'elapsed_time': elapsed_time
                    }
            except requests.exceptions.RequestException as e:
                error_msg = f'API请求失败: {str(e)} (尝试 {attempt + 1}/{self.max_retries})'
                print(f"AI翻译请求失败: {error_msg} - 文本: {text[:50]}...")
                if attempt == self.max_retries - 1:
                    self.stats['failed_requests'] += 1
                    elapsed_time = time.time() - start_time
                    self.stats['total_time'] += elapsed_time
                    return {
                        'success': False,
                        'error': error_msg,
                        'original_text': text,
                        'elapsed_time': elapsed_time
                    }
            except Exception as e:
                error_msg = f'翻译失败: {str(e)} (尝试 {attempt + 1}/{self.max_retries})'
                print(f"AI翻译异常: {error_msg} - 文本: {text[:50]}...")
                if attempt == self.max_retries - 1:
                    self.stats['failed_requests'] += 1
                    elapsed_time = time.time() - start_time
                    self.stats['total_time'] += elapsed_time
                    return {
                        'success': False,
                        'error': error_msg,
                        'original_text': text,
                        'elapsed_time': elapsed_time
                    }
            
            # 重试前等待
            if attempt < self.max_retries - 1:
                wait_time = 2 ** attempt
                print(f"AI翻译等待 {wait_time}s 后重试...")
                time.sleep(wait_time)  # 指数退避
    
    def batch_translate(self, texts, source_lang='en', target_lang='zh', context='medical', batch_size=5):
        """优化的批量翻译，支持真正的批量处理"""
        if not texts:
            return []
        
        results = []
        
        # 过滤空文本
        valid_texts = [(i, text) for i, text in enumerate(texts) if text and text.strip()]
        
        if not valid_texts:
            return [{
                'success': True,
                'translated_text': '',
                'original_text': text,
                'source_lang': source_lang,
                'target_lang': target_lang
            } for text in texts]
        
        # 分批处理
        for batch_start in range(0, len(valid_texts), batch_size):
            batch_end = min(batch_start + batch_size, len(valid_texts))
            batch_items = valid_texts[batch_start:batch_end]
            
            # 构建批量翻译请求
            batch_text = '\n'.join([f'{i+1}. {text}' for i, (_, text) in enumerate(batch_items)])
            
            if context == 'medical':
                system_prompt = f"你是一个专业的临床医学SAS数据集翻译专家。请将以下{source_lang}文本准确翻译为{target_lang}，保持医学术语的专业性和准确性。请按序号返回翻译结果，每行一个。"
            else:
                system_prompt = f"请将以下{source_lang}文本翻译为{target_lang}，保持原意和专业性。请按序号返回翻译结果，每行一个。"
            
            batch_result = self.translate_text(batch_text, source_lang, target_lang, context)
            
            if batch_result.get('success'):
                translated_lines = batch_result.get('translated_text', '').strip().split('\n')
                
                for i, (original_index, original_text) in enumerate(batch_items):
                    if i < len(translated_lines):
                        # 提取翻译结果（去除序号）
                        translated = translated_lines[i].strip()
                        if '. ' in translated and translated.split('.')[0].isdigit():
                            translated = translated.split('. ', 1)[1]
                        
                        results.append((original_index, {
                            'success': True,
                            'translated_text': translated,
                            'original_text': original_text,
                            'source_lang': source_lang,
                            'target_lang': target_lang
                        }))
                    else:
                        # 批量翻译结果不足，回退到单独翻译
                        single_result = self.translate_text(original_text, source_lang, target_lang, context)
                        results.append((original_index, single_result))
            else:
                # 批量翻译失败，回退到单独翻译
                print(f"批量翻译失败，回退到单独翻译: {batch_result.get('error')}")
                for original_index, original_text in batch_items:
                    single_result = self.translate_text(original_text, source_lang, target_lang, context)
                    results.append((original_index, single_result))
        
        # 重新排序结果
        final_results = [None] * len(texts)
        for original_index, result in results:
            final_results[original_index] = result
        
        # 填充空文本的结果
        for i, text in enumerate(texts):
            if final_results[i] is None:
                final_results[i] = {
                    'success': True,
                    'translated_text': '',
                    'original_text': text,
                    'source_lang': source_lang,
                    'target_lang': target_lang
                }
        
        return final_results

# 创建翻译服务实例
translation_service = DeepSeekTranslationService()

@app.route('/api/translate_text', methods=['POST'])
def translate_text():
    """文本翻译API"""
    try:
        data = request.get_json()
        text = data.get('text', '')
        source_lang = data.get('source_lang', 'en')
        target_lang = data.get('target_lang', 'zh')
        context = data.get('context', 'medical')
        
        if not text:
            return jsonify({
                'success': False,
                'error': '文本不能为空'
            }), 400
        
        result = translation_service.translate_text(text, source_lang, target_lang, context)
        return jsonify(result)
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'翻译服务错误: {str(e)}'
        }), 500

@app.route('/api/translation_stats', methods=['GET'])
def get_translation_stats():
    """获取AI翻译统计信息"""
    try:
        stats = translation_service.get_stats()
        return jsonify({
            'success': True,
            'stats': stats
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'获取统计信息失败: {str(e)}'
        }), 500

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

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
