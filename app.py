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
        
    def read_sas_files(self, directory_path, mode='RAW'):
        """读取SAS数据集文件"""
        self.datasets = {}
        self.hide_supp_in_preview = (mode == 'SDTM')
        
        try:
            sas_files = glob.glob(os.path.join(directory_path, "*.sas7bdat"))
            
            for file_path in sas_files:
                dataset_name = Path(file_path).stem
                df, meta = pyreadstat.read_sas7bdat(file_path)
                self.datasets[dataset_name] = {
                    'data': df.copy(),
                    'raw_data': df.copy(),
                    'meta': meta,
                    'path': file_path
                }
            
            if mode == 'SDTM':
                # 仅构建预览视图，不直接改写主表
                self._build_preview_views()
                
            return True, f"成功读取 {len(self.datasets)} 个数据集"
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
        
        if dataset_name:
            # 通过数据集名称获取
            if dataset_name not in processor.datasets:
                return jsonify({'success': False, 'error': f'数据集 {dataset_name} 不存在'})
            
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

@app.route('/api/generate_coded_list', methods=['POST'])
def generate_coded_list():
    """生成编码清单"""
    try:
        data = request.get_json()
        
        # 验证必要字段
        if not data.get('translation_direction') or not data.get('mode'):
            return jsonify({'success': False, 'message': '缺少必要的配置信息'}), 400
        
        # 这里实现编码清单生成逻辑
        # 根据翻译方向、模式和版本信息生成编码清单
        result = {
            'success': True,
            'message': '编码清单生成成功',
            'data': {
                'translation_direction': data.get('translation_direction'),
                'mode': data.get('mode'),
                'meddra_version': data.get('meddra_version'),
                'whodrug_version': data.get('whodrug_version'),
                'ig_version': data.get('ig_version'),
                'coded_items': []  # 这里应该包含实际的编码清单数据
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
        if not data.get('translation_direction') or not data.get('mode'):
            return jsonify({'success': False, 'message': '缺少必要的配置信息'}), 400
        
        # 这里实现非编码清单生成逻辑
        # 根据翻译方向、模式和版本信息生成非编码清单
        result = {
            'success': True,
            'message': '非编码清单生成成功',
            'data': {
                'translation_direction': data.get('translation_direction'),
                'mode': data.get('mode'),
                'meddra_version': data.get('meddra_version'),
                'whodrug_version': data.get('whodrug_version'),
                'ig_version': data.get('ig_version'),
                'uncoded_items': []  # 这里应该包含实际的非编码清单数据
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
        if not data.get('translation_direction') or not data.get('mode'):
            return jsonify({'success': False, 'message': '缺少必要的配置信息'}), 400
        
        # 这里实现数据集Label生成逻辑
        # 根据翻译方向、模式和版本信息生成数据集Label
        result = {
            'success': True,
            'message': '数据集Label生成成功',
            'data': {
                'translation_direction': data.get('translation_direction'),
                'mode': data.get('mode'),
                'meddra_version': data.get('meddra_version'),
                'whodrug_version': data.get('whodrug_version'),
                'ig_version': data.get('ig_version'),
                'dataset_labels': []  # 这里应该包含实际的数据集Label数据
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
        if not data.get('translation_direction') or not data.get('mode'):
            return jsonify({'success': False, 'message': '缺少必要的配置信息'}), 400
        
        # 这里实现变量Label生成逻辑
        # 根据翻译方向、模式和版本信息生成变量Label
        result = {
            'success': True,
            'message': '变量Label生成成功',
            'data': {
                'translation_direction': data.get('translation_direction'),
                'mode': data.get('mode'),
                'meddra_version': data.get('meddra_version'),
                'whodrug_version': data.get('whodrug_version'),
                'ig_version': data.get('ig_version'),
                'variable_labels': []  # 这里应该包含实际的变量Label数据
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
                system_prompt = f"你是一个专业的医学翻译专家。请将以下{source_lang}文本准确翻译为{target_lang}，保持医学术语的专业性和准确性。"
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
        """批量翻译"""
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
        
        text = data.get('text')
        source_lang = data.get('source_lang', 'en')
        target_lang = data.get('target_lang', 'zh')
        context = data.get('context', 'medical')
        
        if not text:
            return jsonify({'success': False, 'message': '缺少要翻译的文本'}), 400
        
        result = translation_service.translate_text(text, source_lang, target_lang, context)
        
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

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
