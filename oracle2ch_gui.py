#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import os
import queue
import re
import sys
import threading
import traceback
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import clickhouse_connect
import oracledb
import pymysql
import tkinter as tk
from tkinter import ttk, messagebox
from tkinter.scrolledtext import ScrolledText


APP_NAME = "Oracle2Doris/ClickHouse 表结构转换工具"
CONFIG_FILE = Path(__file__).with_name("app_config.json")

ORACLE_TABLES_SQL = """
SELECT table_name
FROM all_tables
WHERE owner = :owner
ORDER BY table_name
"""

ORACLE_COLUMNS_SQL = """
SELECT table_name,
       column_name,
       data_type,
       data_length,
       data_precision,
       data_scale,
       nullable,
       column_id,
       char_length,
       char_used
FROM all_tab_columns
WHERE owner = :owner
ORDER BY table_name, column_id
"""

ORACLE_COL_COMMENTS_SQL = """
SELECT table_name,
       column_name,
       comments
FROM all_col_comments
WHERE owner = :owner
"""

ORACLE_TABLE_COMMENTS_SQL = """
SELECT table_name,
       comments
FROM all_tab_comments
WHERE owner = :owner
"""

ORACLE_PK_SQL = """
SELECT acc.table_name,
       acc.column_name,
       acc.position
FROM all_constraints ac
JOIN all_cons_columns acc
  ON ac.owner = acc.owner
 AND ac.constraint_name = acc.constraint_name
WHERE ac.owner = :owner
  AND ac.constraint_type = 'P'
ORDER BY acc.table_name, acc.position
"""


@dataclass
class ColumnMeta:
    name: str
    data_type: str
    data_length: Optional[int]
    data_precision: Optional[int]
    data_scale: Optional[int]
    nullable: str
    char_length: Optional[int]
    char_used: Optional[str] = None
    comment: Optional[str] = None


class SchemaConverter:
    def __init__(self, logger):
        self.logger = logger

    @staticmethod
    def _quote_ident(name: str) -> str:
        return f"`{name.replace('`', '``')}`"

    @staticmethod
    def _escape_string(text: str) -> str:
        return text.replace("\\", "\\\\").replace("'", "\\'")

    def oracle_to_clickhouse_type(self, col: ColumnMeta) -> str:
        dt = (col.data_type or "").upper()
        precision = col.data_precision
        scale = col.data_scale

        if dt in {"CHAR", "NCHAR", "VARCHAR2", "NVARCHAR2", "CLOB", "NCLOB", "LONG"}:
            ch_type = "String"
        elif dt in {"BLOB", "RAW", "LONG RAW"}:
            ch_type = "String"
        elif dt.startswith("TIMESTAMP"):
            ch_type = "DateTime64(3)"
        elif dt == "DATE":
            ch_type = "DateTime"
        elif dt in {"FLOAT", "BINARY_FLOAT"}:
            ch_type = "Float32"
        elif dt == "BINARY_DOUBLE":
            ch_type = "Float64"
        elif dt == "NUMBER":
            ch_type = self._map_number_type(precision, scale)
        elif dt.startswith("INTERVAL"):
            ch_type = "String"
        elif dt in {"XMLTYPE", "JSON"}:
            ch_type = "String"
        else:
            ch_type = "String"

        if (col.nullable or "Y").upper() == "Y":
            return f"Nullable({ch_type})"
        return ch_type

    @staticmethod
    def _map_number_type(precision: Optional[int], scale: Optional[int]) -> str:
        if precision is None and scale is None:
            return "Float64"
        if scale is not None and scale > 0:
            p = precision if precision is not None else 38
            p = max(1, min(76, p))
            s = max(0, min(scale, p))
            return f"Decimal({p}, {s})"

        if precision is None:
            return "Int64"
        if precision <= 2:
            return "Int8"
        if precision <= 4:
            return "Int16"
        if precision <= 9:
            return "Int32"
        if precision <= 18:
            return "Int64"
        if precision <= 38:
            return "Int128"
        return "Int256"

    def oracle_to_doris_type(self, col: ColumnMeta) -> str:
        dt = (col.data_type or "").upper()
        precision = col.data_precision
        scale = col.data_scale

        if dt in {"CHAR", "NCHAR"}:
            char_len = self._oracle_string_bytes_for_doris(col, fixed=True)
            if char_len > 255:
                return f"VARCHAR({min(65533, char_len)})" if char_len <= 65533 else "STRING"
            return f"CHAR({max(1, char_len)})"
        if dt in {"VARCHAR2", "NVARCHAR2"}:
            char_len = self._oracle_string_bytes_for_doris(col, national=dt.startswith("N"))
            return f"VARCHAR({char_len})" if char_len <= 65533 else "STRING"
        if dt in {"CLOB", "NCLOB", "LONG", "BLOB", "RAW", "LONG RAW", "XMLTYPE", "JSON"}:
            return "STRING"
        if dt.startswith("TIMESTAMP") or dt == "DATE":
            return "DATETIME"
        if dt in {"FLOAT", "BINARY_FLOAT"}:
            return "FLOAT"
        if dt == "BINARY_DOUBLE":
            return "DOUBLE"
        if dt == "NUMBER":
            return self._map_number_type_for_doris(precision, scale)
        if dt.startswith("INTERVAL"):
            return "STRING"
        return "STRING"

    def oracle_to_mysql8_type(self, col: ColumnMeta) -> str:
        dt = (col.data_type or "").upper()
        precision = col.data_precision
        scale = col.data_scale

        if dt in {"CHAR", "NCHAR"}:
            return f"CHAR({max(1, min(255, col.char_length or col.data_length or 255))})"
        if dt in {"VARCHAR2", "NVARCHAR2"}:
            length = max(1, col.char_length or col.data_length or 255)
            return f"VARCHAR({length})" if length <= 16383 else "LONGTEXT"
        if dt in {"CLOB", "NCLOB", "LONG", "XMLTYPE", "JSON"}:
            return "LONGTEXT"
        if dt in {"BLOB", "RAW", "LONG RAW"}:
            return "LONGBLOB"
        if dt.startswith("TIMESTAMP"):
            return "DATETIME(6)"
        if dt == "DATE":
            return "DATETIME"
        if dt in {"FLOAT", "BINARY_FLOAT"}:
            return "FLOAT"
        if dt == "BINARY_DOUBLE":
            return "DOUBLE"
        if dt == "NUMBER":
            return self._map_number_type_for_mysql8(precision, scale)
        if dt.startswith("INTERVAL"):
            return "VARCHAR(255)"
        return "LONGTEXT"

    @staticmethod
    def _oracle_string_bytes_for_doris(
        col: ColumnMeta, national: bool = False, fixed: bool = False
    ) -> int:
        length = col.char_length or col.data_length or 255
        # ponytail: Doris VARCHAR length is bytes; x3 keeps Chinese UTF-8 data from being truncated.
        if not fixed and (national or (col.char_used or "").upper() == "C"):
            length *= 3
        return max(1, length)

    @staticmethod
    def _map_number_type_for_doris(precision: Optional[int], scale: Optional[int]) -> str:
        if precision is None and scale is None:
            return "DOUBLE"
        if scale is not None and scale > 0:
            p = max(1, min(38, precision if precision is not None else 38))
            s = max(0, min(scale, p))
            return f"DECIMAL({p}, {s})"

        if precision is None:
            return "BIGINT"
        if precision <= 2:
            return "TINYINT"
        if precision <= 4:
            return "SMALLINT"
        if precision <= 9:
            return "INT"
        if precision <= 18:
            return "BIGINT"
        return "LARGEINT"

    @staticmethod
    def _map_number_type_for_mysql8(precision: Optional[int], scale: Optional[int]) -> str:
        if precision is None and scale is None:
            return "DOUBLE"
        if scale is not None and scale > 0:
            p = max(1, min(65, precision if precision is not None else 38))
            s = max(0, min(scale, 30, p))
            return f"DECIMAL({p}, {s})"

        if precision is None:
            return "BIGINT"
        if precision <= 2:
            return "TINYINT"
        if precision <= 4:
            return "SMALLINT"
        if precision <= 9:
            return "INT"
        if precision <= 18:
            return "BIGINT"
        return "DECIMAL(38, 0)"

    def build_create_table_sql(
        self,
        ch_database: str,
        table_name: str,
        columns: Sequence[ColumnMeta],
        pk_columns: Sequence[str],
        target: str = "clickhouse",
        table_comment: Optional[str] = None,
    ) -> str:
        if target.lower() == "doris":
            return self.build_doris_create_table_sql(
                ch_database, table_name, columns, pk_columns, table_comment
            )
        return self.build_clickhouse_create_table_sql(ch_database, table_name, columns, pk_columns)

    def build_clickhouse_create_table_sql(
        self,
        ch_database: str,
        table_name: str,
        columns: Sequence[ColumnMeta],
        pk_columns: Sequence[str],
    ) -> str:
        col_sql: List[str] = []
        for col in columns:
            ch_type = self.oracle_to_clickhouse_type(col)
            col_sql.append(f"  {self._quote_ident(col.name)} {ch_type}")

        if pk_columns:
            order_by = ", ".join(self._quote_ident(c) for c in pk_columns)
            order_by_expr = f"({order_by})"
        else:
            order_by_expr = "tuple()"

        db = self._quote_ident(ch_database)
        table = self._quote_ident(table_name)
        sql = (
            f"CREATE TABLE IF NOT EXISTS {db}.{table} (\n"
            + ",\n".join(col_sql)
            + "\n)\nENGINE = MergeTree\n"
            f"ORDER BY {order_by_expr}"
        )
        return sql

    def build_doris_create_table_sql(
        self,
        database: str,
        table_name: str,
        columns: Sequence[ColumnMeta],
        pk_columns: Sequence[str],
        table_comment: Optional[str] = None,
    ) -> str:
        col_sql: List[str] = []
        for col in columns:
            doris_type = self.oracle_to_doris_type(col)
            null_sql = "NULL" if (col.nullable or "Y").upper() == "Y" else "NOT NULL"
            comment_sql = (
                f" COMMENT '{self._escape_string(col.comment)}'" if col.comment else ""
            )
            col_sql.append(f"  {self._quote_ident(col.name)} {doris_type} {null_sql}{comment_sql}")

        key_columns = self._doris_key_columns(columns, pk_columns)
        key_expr = ", ".join(self._quote_ident(c) for c in key_columns)
        db = self._quote_ident(database)
        table = self._quote_ident(table_name)

        sql = f"CREATE TABLE IF NOT EXISTS {db}.{table} (\n" + ",\n".join(col_sql) + "\n)"
        if key_expr:
            sql += (
                "\nENGINE=OLAP"
                + f"\nDUPLICATE KEY({key_expr})\n"
                + (f"COMMENT '{self._escape_string(table_comment)}'\n" if table_comment else "")
                + f"DISTRIBUTED BY HASH({key_expr}) BUCKETS 10"
            )
        elif table_comment:
            sql += f"\nCOMMENT '{self._escape_string(table_comment)}'"
        return sql

    def _doris_key_columns(self, columns: Sequence[ColumnMeta], pk_columns: Sequence[str]) -> List[str]:
        by_name = {c.name.upper(): c for c in columns}
        candidates = list(pk_columns) or [c.name for c in columns]
        keys: List[str] = []
        for name in candidates:
            col = by_name.get(name.upper())
            if col is None:
                continue
            if self.oracle_to_doris_type(col) == "STRING":
                continue
            keys.append(col.name)
            if len(keys) == 3:
                break
        return keys

    def fetch_oracle_schema(
        self,
        oracle_cfg: Dict[str, str],
        selected_tables: Optional[Sequence[str]] = None,
    ) -> Tuple[List[str], Dict[str, List[ColumnMeta]], Dict[str, List[str]], Dict[str, str]]:
        owner = oracle_cfg["schema"].upper().strip()
        dsn = oracledb.makedsn(
            oracle_cfg["host"].strip(),
            int(oracle_cfg["port"]),
            service_name=oracle_cfg["service_name"].strip(),
        )
        tables_filter = {t.upper().strip() for t in selected_tables or [] if t.strip()}

        self.logger(f"连接 Oracle: {oracle_cfg['host']}:{oracle_cfg['port']}/{oracle_cfg['service_name']}")
        with oracledb.connect(
            user=oracle_cfg["user"].strip(),
            password=oracle_cfg["password"],
            dsn=dsn,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(ORACLE_TABLES_SQL, owner=owner)
                all_tables = [r[0] for r in cur.fetchall()]
                if tables_filter:
                    tables = [t for t in all_tables if t.upper() in tables_filter]
                else:
                    tables = all_tables

                if not tables:
                    raise RuntimeError("未找到可转换的表，请检查 Schema 或表名过滤条件。")

                table_set = set(tables)
                table_columns: Dict[str, List[ColumnMeta]] = {t: [] for t in tables}

                cur.execute(ORACLE_COL_COMMENTS_SQL, owner=owner)
                col_comments = {
                    (row[0], row[1]): row[2]
                    for row in cur.fetchall()
                    if row[0] in table_set and row[2]
                }

                cur.execute(ORACLE_TABLE_COMMENTS_SQL, owner=owner)
                table_comments = {
                    row[0]: row[1]
                    for row in cur.fetchall()
                    if row[0] in table_set and row[1]
                }

                cur.execute(ORACLE_COLUMNS_SQL, owner=owner)
                for row in cur.fetchall():
                    table_name = row[0]
                    if table_name not in table_set:
                        continue
                    table_columns[table_name].append(
                        ColumnMeta(
                            name=row[1],
                            data_type=row[2],
                            data_length=row[3],
                            data_precision=row[4],
                            data_scale=row[5],
                            nullable=row[6],
                            char_length=row[8],
                            char_used=row[9],
                            comment=col_comments.get((table_name, row[1])),
                        )
                    )

                cur.execute(ORACLE_PK_SQL, owner=owner)
                pk_map: Dict[str, List[str]] = {t: [] for t in tables}
                for row in cur.fetchall():
                    table_name, col_name, _ = row
                    if table_name in table_set:
                        pk_map[table_name].append(col_name)

        return tables, table_columns, pk_map, table_comments

    def fetch_oracle_tables(
        self,
        oracle_cfg: Dict[str, str],
        prefix: str = "",
    ) -> List[Tuple[str, str]]:
        owner = oracle_cfg["schema"].upper().strip()
        dsn = oracledb.makedsn(
            oracle_cfg["host"].strip(),
            int(oracle_cfg["port"]),
            service_name=oracle_cfg["service_name"].strip(),
        )
        prefix = prefix.upper().strip()
        sql = (
            "SELECT t.table_name, c.comments "
            "FROM all_tables t "
            "LEFT JOIN all_tab_comments c ON c.owner = t.owner AND c.table_name = t.table_name "
            "WHERE t.owner = :owner "
        )
        params = {"owner": owner}
        if prefix:
            sql += "AND t.table_name LIKE :prefix "
            params["prefix"] = prefix + "%"
        sql += "ORDER BY t.table_name"

        with oracledb.connect(
            user=oracle_cfg["user"].strip(),
            password=oracle_cfg["password"],
            dsn=dsn,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return [(row[0], row[1] or "") for row in cur.fetchall()]


class App:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title(APP_NAME)
        self.root.geometry("1280x880")
        self.root.minsize(1120, 760)
        self.ui_queue: queue.Queue = queue.Queue()
        self.running = False

        self.converter = SchemaConverter(self.log)
        self.vars: Dict[str, tk.StringVar] = {}
        self.bool_vars: Dict[str, tk.BooleanVar] = {}
        self.table_rows: Dict[str, Tuple[str, str]] = {}
        self.selected_tables: set = set()
        self.oracle_client_inited = False

        self._build_ui()
        self._load_config(silent=True)
        self.root.after(150, self._process_ui_queue)

    def _apply_styles(self):
        self.root.configure(bg="#f4f6f8")
        style = ttk.Style(self.root)
        style.configure(".", font=("Microsoft YaHei UI", 10))
        style.configure("TFrame", background="#f4f6f8")
        style.configure("Hero.TFrame", background="#203040")
        style.configure("HeroTitle.TLabel", background="#203040", foreground="#ffffff", font=("Microsoft YaHei UI", 18, "bold"))
        style.configure("HeroSub.TLabel", background="#203040", foreground="#c8d3dc")
        style.configure("TLabel", background="#ffffff", foreground="#26323d")
        style.configure("TLabelframe", background="#ffffff", relief="solid", bordercolor="#d9e0e7")
        style.configure("TLabelframe.Label", background="#ffffff", foreground="#203040", font=("Microsoft YaHei UI", 11, "bold"))
        style.configure("TEntry", padding=3)
        style.configure("TCombobox", padding=2)
        style.configure("TCheckbutton", background="#ffffff", foreground="#26323d")
        style.configure("TButton", padding=(10, 5))
        style.configure("Compact.TButton", padding=(8, 4), font=("Microsoft YaHei UI", 9))
        style.configure("Primary.TButton", padding=(16, 7), font=("Microsoft YaHei UI", 10, "bold"))
        style.configure("Horizontal.TProgressbar", troughcolor="#e6ebf0", background="#2f7d68", thickness=12)

    def _build_ui(self):
        self._apply_styles()
        outer = ttk.Frame(self.root, padding=16)
        outer.pack(fill="both", expand=True)

        hero = ttk.Frame(outer, style="Hero.TFrame", padding=(18, 10))
        hero.pack(fill="x", pady=(0, 10))
        ttk.Label(hero, text="Oracle Schema Converter", style="HeroTitle.TLabel").pack(anchor="w")
        ttk.Label(hero, text="Oracle 表结构转换为 ClickHouse / Doris 建表 SQL", style="HeroSub.TLabel").pack(anchor="w", pady=(4, 0))

        body = ttk.Frame(outer)
        body.pack(fill="both", expand=True)
        right = ttk.Frame(body)
        right.pack(side="right", fill="both", padx=(10, 0))
        right.pack_propagate(False)
        right.configure(width=540)
        left = ttk.Frame(body)
        left.pack(side="left", fill="both", expand=True)

        oracle_frame = ttk.LabelFrame(left, text="Oracle 连接信息", padding=(8, 6))
        oracle_frame.pack(fill="x")
        target_frame = ttk.LabelFrame(left, text="目标库连接信息", padding=(8, 6))
        target_frame.pack(fill="x", pady=(6, 0))
        option_frame = ttk.LabelFrame(left, text="转换选项", padding=10)
        option_frame.pack(fill="both", expand=True, pady=(6, 0))
        progress_frame = ttk.LabelFrame(right, text="转换进度", padding=10)
        progress_frame.pack(fill="x")
        log_frame = ttk.LabelFrame(right, text="转换日志", padding=10)
        log_frame.pack(fill="both", expand=True, pady=(8, 0))

        self._build_oracle_inputs(oracle_frame)
        self._build_click_inputs(target_frame)
        self._build_options(option_frame)
        self._build_progress(progress_frame)
        self._build_logs(log_frame)

    def _build_oracle_inputs(self, parent: ttk.Frame):
        self._prep_form(parent)
        self._add_compact_entry(parent, "oracle_host", "Host", 0, default="192.168.6.66")
        self._add_compact_entry(parent, "oracle_port", "Port", 0, 2, default="1521")
        self._add_compact_entry(parent, "oracle_service", "Service Name", 0, 4, default="PHXORCL")
        self._add_compact_entry(parent, "oracle_schema", "Schema", 1, default="HISOPT")
        self._add_compact_entry(parent, "oracle_user", "User", 1, 2, default="hisopt")
        self._add_compact_entry(parent, "oracle_password", "Password", 1, 4, show="*", default="hisopt")
        self._add_compact_entry(parent, "oracle_client_dir", "Instant Client 路径(可选)", 2, width=38, columnspan=3)
        self.bool_vars["oracle_thick_mode"] = tk.BooleanVar(value=True)
        tk.Checkbutton(
            parent,
            text="启用 Oracle Thick 模式(旧版本数据库建议开启)",
            variable=self.bool_vars["oracle_thick_mode"],
            bg="#ffffff",
            activebackground="#ffffff",
            anchor="w",
            padx=0,
            pady=0,
        ).grid(row=2, column=4, columnspan=2, sticky="w", pady=1)
        ttk.Button(parent, text="测试连接", command=self.test_oracle_conn, style="Compact.TButton").grid(
            row=0, column=6, rowspan=3, padx=8, sticky="ns"
        )

    def _build_click_inputs(self, parent: ttk.Frame):
        self._prep_form(parent)
        ttk.Label(parent, text="数据库类型").grid(row=0, column=0, sticky="w", padx=(0, 6), pady=3)
        self.vars["target_db"] = tk.StringVar(value="clickhouse")
        ttk.Combobox(
            parent,
            textvariable=self.vars["target_db"],
            values=("clickhouse", "doris"),
            width=18,
            state="readonly",
        ).grid(row=0, column=1, sticky="we", padx=(0, 12), pady=3)
        self._add_compact_entry(parent, "ch_host", "Host", 1, default="192.168.6.199")
        self._add_compact_entry(parent, "ch_port", "Port", 1, 2, default="8123")
        self._add_compact_entry(parent, "ch_database", "Database", 1, 4, default="corexdb")
        self._add_compact_entry(parent, "ch_user", "User", 2, default="hisqry")
        self._add_compact_entry(parent, "ch_password", "Password", 2, 2, show="*")
        ttk.Button(parent, text="测试连接", command=self.test_clickhouse_conn, style="Compact.TButton").grid(
            row=1, column=6, rowspan=2, padx=8, sticky="ns"
        )

    def _build_options(self, parent: ttk.Frame):
        self._prep_form(parent)
        self._add_entry(
            parent,
            "table_filter",
            "表名前缀过滤",
            0,
            width=30,
            columnspan=3,
            default="",
        )
        ttk.Button(parent, text="查询表", command=self.query_oracle_tables).grid(row=0, column=4, sticky="w", padx=(0, 8), pady=6)
        ttk.Button(parent, text="全选", command=self.select_all_tables).grid(row=0, column=5, sticky="w", padx=(0, 8), pady=6)
        ttk.Button(parent, text="取消全选", command=self.clear_table_selection).grid(row=0, column=6, sticky="w", pady=6)

        table_box = ttk.Frame(parent)
        table_box.grid(row=1, column=0, columnspan=8, sticky="nsew", pady=(8, 6))
        parent.rowconfigure(1, weight=1)
        table_box.columnconfigure(0, weight=1)
        table_box.rowconfigure(0, weight=1)
        self.table_tree = ttk.Treeview(
            table_box,
            columns=("selected", "name", "comment"),
            show="headings",
            height=14,
            selectmode="browse",
        )
        self.table_tree.heading("selected", text="选择")
        self.table_tree.heading("name", text="表名称")
        self.table_tree.heading("comment", text="表注释")
        self.table_tree.column("selected", width=42, anchor="center", stretch=False)
        self.table_tree.column("name", width=260, minwidth=220, anchor="w", stretch=False)
        self.table_tree.column("comment", width=300, minwidth=220, anchor="w")
        self.table_tree.grid(row=0, column=0, sticky="nsew")
        table_yscroll = ttk.Scrollbar(table_box, orient="vertical", command=self.table_tree.yview)
        table_yscroll.grid(row=0, column=1, sticky="ns")
        table_xscroll = ttk.Scrollbar(table_box, orient="horizontal", command=self.table_tree.xview)
        table_xscroll.grid(row=1, column=0, sticky="ew")
        self.table_tree.configure(yscrollcommand=table_yscroll.set, xscrollcommand=table_xscroll.set)
        self.table_tree.bind("<ButtonRelease-1>", self.toggle_table_selection)

        self.bool_vars["execute_on_clickhouse"] = tk.BooleanVar(value=False)
        self.bool_vars["create_db_if_not_exists"] = tk.BooleanVar(value=False)
        self.bool_vars["skip_existing_tables"] = tk.BooleanVar(value=False)
        tk.Checkbutton(
            parent,
            text="执行建表到目标库",
            variable=self.bool_vars["execute_on_clickhouse"],
            bg="#ffffff",
            activebackground="#ffffff",
            anchor="w",
        ).grid(row=2, column=0, sticky="w", pady=(4, 0))
        tk.Checkbutton(
            parent,
            text="自动创建数据库 (IF NOT EXISTS)",
            variable=self.bool_vars["create_db_if_not_exists"],
            bg="#ffffff",
            activebackground="#ffffff",
            anchor="w",
        ).grid(row=2, column=1, columnspan=2, sticky="w", pady=(4, 0))
        tk.Checkbutton(
            parent,
            text="跳过目标库已存在表",
            variable=self.bool_vars["skip_existing_tables"],
            bg="#ffffff",
            activebackground="#ffffff",
            anchor="w",
        ).grid(row=2, column=3, columnspan=2, sticky="w", pady=(4, 0))

        btn_row = ttk.Frame(parent)
        btn_row.grid(row=3, column=0, columnspan=8, sticky="w", pady=(12, 0))
        self.start_btn = ttk.Button(btn_row, text="开始转换", command=self.start_convert, style="Primary.TButton")
        self.start_btn.pack(side="left")
        ttk.Button(btn_row, text="保存配置", command=self.save_config).pack(side="left", padx=(8, 0))
        ttk.Button(btn_row, text="加载配置", command=lambda: self._load_config(silent=False)).pack(side="left", padx=(8, 0))
    def _build_progress(self, parent: ttk.Frame):
        self.progress_var = tk.DoubleVar(value=0.0)
        self.progress = ttk.Progressbar(parent, variable=self.progress_var, maximum=100)
        self.progress.pack(fill="x")
        self.progress_label = ttk.Label(parent, text="等待开始")
        self.progress_label.pack(anchor="w", pady=(8, 0))

    def _build_logs(self, parent: ttk.Frame):
        self.log_text = ScrolledText(
            parent,
            height=24,
            wrap="word",
            bg="#0f1720",
            fg="#d7e0e8",
            insertbackground="#d7e0e8",
            relief="flat",
            padx=10,
            pady=10,
        )
        self.log_text.pack(fill="both", expand=True)
        self.log_text.configure(state="disabled")

    def _add_entry(
        self,
        parent: ttk.Frame,
        key: str,
        label: str,
        row: int,
        col: int = 0,
        default: str = "",
        width: int = 24,
        show: Optional[str] = None,
        columnspan: int = 1,
    ):
        ttk.Label(parent, text=label).grid(row=row, column=col, sticky="w", padx=(0, 6), pady=6)
        var = tk.StringVar(value=default)
        ent = ttk.Entry(parent, textvariable=var, width=width, show=show)
        ent.grid(row=row, column=col + 1, sticky="we", padx=(0, 12), pady=6, columnspan=columnspan)
        self.vars[key] = var

    def _add_compact_entry(
        self,
        parent: ttk.Frame,
        key: str,
        label: str,
        row: int,
        col: int = 0,
        default: str = "",
        width: int = 20,
        show: Optional[str] = None,
        columnspan: int = 1,
    ):
        ttk.Label(parent, text=label).grid(row=row, column=col, sticky="w", padx=(0, 5), pady=3)
        var = tk.StringVar(value=default)
        ent = ttk.Entry(parent, textvariable=var, width=width, show=show)
        ent.grid(row=row, column=col + 1, sticky="we", padx=(0, 8), pady=3, columnspan=columnspan)
        self.vars[key] = var

    def _prep_form(self, parent: ttk.Frame):
        for col in (1, 3, 5):
            parent.columnconfigure(col, weight=1)

    def _set_table_selected(self, table_name: str, selected: bool):
        if selected:
            self.selected_tables.add(table_name)
        else:
            self.selected_tables.discard(table_name)
        if hasattr(self, "table_tree"):
            mark = "✓" if selected else ""
            comment = self.table_rows.get(table_name, ("", ""))[1]
            self.table_tree.item(table_name, values=(mark, table_name, comment))

    def _load_table_rows(self, rows: Sequence[Tuple[str, str]]):
        self.table_rows = {name: (name, comment) for name, comment in rows}
        self.selected_tables = set(self.table_rows)
        self.table_tree.delete(*self.table_tree.get_children())
        for name, comment in rows:
            self.table_tree.insert("", "end", iid=name, values=("✓", name, comment))

    def query_oracle_tables(self):
        cfg = self._collect_config()
        try:
            self._validate_oracle_config(cfg["oracle"])
        except Exception as exc:
            messagebox.showerror("参数错误", str(exc))
            return
        self._run_background(self._query_oracle_tables_worker, cfg, "表清单查询完成")

    def _query_oracle_tables_worker(self, cfg: Dict[str, Dict[str, str]]):
        self._ensure_oracle_client_mode(cfg["oracle"])
        rows = self.converter.fetch_oracle_tables(
            cfg["oracle"], prefix=cfg["options"]["table_filter"]
        )
        self.ui_queue.put(("tables", rows))
        self.log(f"查询到 {len(rows)} 张表。")

    def toggle_table_selection(self, _event=None):
        if not hasattr(self, "table_tree"):
            return
        item = self.table_tree.focus()
        if item:
            self._set_table_selected(item, item not in self.selected_tables)

    def select_all_tables(self):
        for table_name in list(self.table_rows):
            self._set_table_selected(table_name, True)

    def clear_table_selection(self):
        for table_name in list(self.table_rows):
            self._set_table_selected(table_name, False)

    @staticmethod
    def _validate_oracle_config(oracle_cfg: Dict[str, str]):
        required = [
            ("Oracle Host", oracle_cfg["host"]),
            ("Oracle Port", oracle_cfg["port"]),
            ("Oracle Service Name", oracle_cfg["service_name"]),
            ("Oracle Schema", oracle_cfg["schema"]),
            ("Oracle User", oracle_cfg["user"]),
        ]
        missing = [name for name, value in required if not value]
        if missing:
            raise ValueError("以下参数不能为空: " + ", ".join(missing))
        if not re.match(r"^\d+$", oracle_cfg["port"]):
            raise ValueError("Oracle Port 必须是数字")

    def _append_log(self, msg: str):
        self.log_text.configure(state="normal")
        self.log_text.insert("end", msg + "\n")
        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    def log(self, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        self.ui_queue.put(("log", f"[{ts}] {msg}"))

    def _set_progress(self, current: int, total: int, table_name: str):
        total = max(1, total)
        percent = round(current * 100 / total, 2)
        self.ui_queue.put(("progress", percent, current, total, table_name))

    def _set_running(self, running: bool):
        self.running = running
        state = "disabled" if running else "normal"
        self.start_btn.configure(state=state)

    def _process_ui_queue(self):
        try:
            while True:
                item = self.ui_queue.get_nowait()
                action = item[0]
                if action == "log":
                    self._append_log(item[1])
                elif action == "progress":
                    percent, current, total, table = item[1], item[2], item[3], item[4]
                    self.progress_var.set(percent)
                    self.progress_label.configure(text=f"{current}/{total} ({percent}%) - {table}")
                elif action == "tables":
                    self._load_table_rows(item[1])
                elif action == "done":
                    self._set_running(False)
                    messagebox.showinfo("完成", item[1])
                elif action == "error":
                    self._set_running(False)
                    messagebox.showerror("错误", item[1])
        except queue.Empty:
            pass
        self.root.after(150, self._process_ui_queue)

    def _collect_config(self) -> Dict[str, Dict[str, str]]:
        oracle = {
            "host": self.vars["oracle_host"].get().strip(),
            "port": self.vars["oracle_port"].get().strip(),
            "service_name": self.vars["oracle_service"].get().strip(),
            "schema": self.vars["oracle_schema"].get().strip(),
            "user": self.vars["oracle_user"].get().strip(),
            "password": self.vars["oracle_password"].get(),
            "thick_mode": self.bool_vars["oracle_thick_mode"].get(),
            "client_dir": self.vars["oracle_client_dir"].get().strip(),
        }
        clickhouse = {
            "host": self.vars["ch_host"].get().strip(),
            "port": self.vars["ch_port"].get().strip(),
            "database": self.vars["ch_database"].get().strip(),
            "user": self.vars["ch_user"].get().strip(),
            "password": self.vars["ch_password"].get(),
        }
        options = {
            "target_db": self.vars["target_db"].get().strip().lower() or "clickhouse",
            "table_filter": self.vars["table_filter"].get().strip(),
            "execute_on_clickhouse": self.bool_vars["execute_on_clickhouse"].get(),
            "create_db_if_not_exists": self.bool_vars["create_db_if_not_exists"].get(),
            "skip_existing_tables": self.bool_vars["skip_existing_tables"].get(),
        }
        return {"oracle": oracle, "clickhouse": clickhouse, "options": options}

    def _validate_config(self, cfg: Dict[str, Dict[str, str]]):
        target_db = cfg["options"].get("target_db", "clickhouse")
        required = [
            ("Oracle Host", cfg["oracle"]["host"]),
            ("Oracle Port", cfg["oracle"]["port"]),
            ("Oracle Service Name", cfg["oracle"]["service_name"]),
            ("Oracle Schema", cfg["oracle"]["schema"]),
            ("Oracle User", cfg["oracle"]["user"]),
            ("ClickHouse Database", cfg["clickhouse"]["database"]),
        ]
        if target_db in {"clickhouse", "doris"}:
            required.extend(
                [
                    ("Target Host", cfg["clickhouse"]["host"]),
                    ("Target Port", cfg["clickhouse"]["port"]),
                    ("Target User", cfg["clickhouse"]["user"]),
                ]
            )
        missing = [name for name, value in required if not value]
        if missing:
            raise ValueError("以下参数不能为空: " + ", ".join(missing))
        if target_db not in {"clickhouse", "doris"}:
            raise ValueError("Target must be clickhouse or doris")
        if not re.match(r"^\d+$", cfg["oracle"]["port"]):
            raise ValueError("Oracle Port 必须是数字")
        if target_db in {"clickhouse", "doris"} and not re.match(r"^\d+$", cfg["clickhouse"]["port"]):
            raise ValueError("Target Port 必须是数字")

    def test_oracle_conn(self):
        cfg = self._collect_config()
        self._run_background(self._test_oracle_conn_worker, cfg["oracle"], "Oracle 连接测试通过")

    def _test_oracle_conn_worker(self, oracle_cfg: Dict[str, str]):
        self._ensure_oracle_client_mode(oracle_cfg)
        dsn = oracledb.makedsn(
            oracle_cfg["host"],
            int(oracle_cfg["port"]),
            service_name=oracle_cfg["service_name"],
        )
        try:
            with oracledb.connect(
                user=oracle_cfg["user"], password=oracle_cfg["password"], dsn=dsn
            ) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1 FROM dual")
                    _ = cur.fetchone()
        except Exception as exc:
            msg = str(exc)
            if "DPY-3010" in msg:
                raise RuntimeError(
                    "Oracle 版本较低，Thin 模式不支持。请启用 Thick 模式，并配置 Instant Client 路径。"
                ) from exc
            raise
        self.log("Oracle 连接成功。")

    def _ensure_oracle_client_mode(self, oracle_cfg: Dict[str, str]):
        thick_mode = bool(oracle_cfg.get("thick_mode"))
        if not thick_mode or self.oracle_client_inited:
            return
        client_dir = (oracle_cfg.get("client_dir") or "").strip()
        try:
            if client_dir:
                oracledb.init_oracle_client(lib_dir=client_dir)
                self.log(f"已初始化 Oracle Thick 模式，Instant Client: {client_dir}")
            else:
                oracledb.init_oracle_client()
                self.log("已初始化 Oracle Thick 模式，使用系统 PATH 中的 Instant Client。")
            self.oracle_client_inited = True
        except Exception as exc:
            raise RuntimeError(
                "Oracle Thick 模式初始化失败，请确认 Instant Client 路径正确，或已加入 PATH。"
            ) from exc

    def test_clickhouse_conn(self):
        cfg = self._collect_config()
        target_db = cfg["options"].get("target_db", "clickhouse")
        self._run_background(
            self._test_target_conn_worker,
            (target_db, cfg["clickhouse"]),
            f"{target_db} 连接测试通过",
        )

    def _test_target_conn_worker(self, args):
        target_db, ch_cfg = args
        client = self._open_target_client(target_db, ch_cfg)
        try:
            if target_db == "doris":
                with client.cursor() as cur:
                    cur.execute("SELECT 1")
                    _ = cur.fetchone()[0]
            else:
                result = client.query("SELECT 1")
                _ = result.result_rows[0][0]
            self.log(f"{target_db} 连接成功。")
        finally:
            client.close()

    def _open_target_client(self, target_db: str, cfg: Dict[str, str]):
        if target_db == "doris":
            return pymysql.connect(
                host=cfg["host"],
                port=int(cfg["port"]),
                user=cfg["user"],
                password=cfg["password"],
                database=cfg["database"],
                charset="utf8mb4",
                autocommit=True,
            )
        return clickhouse_connect.get_client(
            host=cfg["host"],
            port=int(cfg["port"]),
            database=cfg["database"],
            username=cfg["user"],
            password=cfg["password"],
        )

    def _target_command(self, client, target_db: str, sql: str):
        if target_db == "doris":
            with client.cursor() as cur:
                cur.execute(sql)
            return
        client.command(sql)

    def _target_existing_tables(self, client, target_db: str, database: str) -> set:
        if target_db == "doris":
            with client.cursor() as cur:
                cur.execute(
                    "SELECT TABLE_NAME FROM information_schema.tables WHERE table_schema = %s",
                    (database,),
                )
                return {str(row[0]).upper() for row in cur.fetchall()}
        db_escaped = database.replace("\\", "\\\\").replace("'", "\\'")
        rows = client.query(f"SELECT name FROM system.tables WHERE database = '{db_escaped}'").result_rows
        return {str(r[0]).upper() for r in rows}

    def _run_background(self, target, arg, done_message: str):
        if self.running:
            messagebox.showwarning("提示", "当前有任务正在执行，请稍后。")
            return

        def runner():
            try:
                target(arg)
                self.ui_queue.put(("done", done_message))
            except Exception as exc:
                self.log(traceback.format_exc())
                self.ui_queue.put(("error", f"{exc}"))

        self._set_running(True)
        threading.Thread(target=runner, daemon=True).start()

    def save_config(self):
        cfg = self._collect_config()
        self._validate_config(cfg)
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
        self.log(f"配置已保存到 {CONFIG_FILE}")
        messagebox.showinfo("提示", "配置已保存")

    def _load_config(self, silent: bool):
        if not CONFIG_FILE.exists():
            if not silent:
                messagebox.showwarning("提示", f"配置文件不存在: {CONFIG_FILE}")
            return
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            cfg = json.load(f)

        oracle = cfg.get("oracle", {})
        clickhouse = cfg.get("clickhouse", {})
        options = cfg.get("options", {})

        key_map = {
            "oracle_host": oracle.get("host", ""),
            "oracle_port": oracle.get("port", ""),
            "oracle_service": oracle.get("service_name", ""),
            "oracle_schema": oracle.get("schema", ""),
            "oracle_user": oracle.get("user", ""),
            "oracle_password": oracle.get("password", ""),
            "oracle_client_dir": oracle.get("client_dir", ""),
            "ch_host": clickhouse.get("host", ""),
            "ch_port": clickhouse.get("port", ""),
            "ch_database": clickhouse.get("database", ""),
            "ch_user": clickhouse.get("user", ""),
            "ch_password": clickhouse.get("password", ""),
            "target_db": options.get("target_db", "clickhouse"),
            "table_filter": options.get("table_filter", ""),
        }
        for key, value in key_map.items():
            if key in self.vars:
                self.vars[key].set(value)

        self.bool_vars["oracle_thick_mode"].set(oracle.get("thick_mode", True))
        self.bool_vars["execute_on_clickhouse"].set(options.get("execute_on_clickhouse", False))
        self.bool_vars["create_db_if_not_exists"].set(options.get("create_db_if_not_exists", False))
        self.bool_vars["skip_existing_tables"].set(options.get("skip_existing_tables", False))

        self.log(f"配置已加载: {CONFIG_FILE}")
        if not silent:
            messagebox.showinfo("提示", "配置已加载")

    def start_convert(self):
        if self.running:
            messagebox.showwarning("提示", "当前有任务正在执行，请稍后。")
            return

        cfg = self._collect_config()
        try:
            self._validate_config(cfg)
        except Exception as exc:
            messagebox.showerror("参数错误", str(exc))
            return

        self._set_running(True)
        self.progress_var.set(0)
        self.progress_label.configure(text="准备开始...")

        worker = threading.Thread(target=self._convert_worker, args=(cfg,), daemon=True)
        worker.start()

    def _convert_worker(self, cfg: Dict[str, Dict[str, str]]):
        try:
            oracle_cfg = cfg["oracle"]
            ch_cfg = cfg["clickhouse"]
            options = cfg["options"]
            self._ensure_oracle_client_mode(oracle_cfg)

            if self.table_rows:
                if not self.selected_tables:
                    raise RuntimeError("请至少选择一张表。")
                selected_tables = sorted(self.selected_tables)
            else:
                prefix = options["table_filter"].strip()
                selected_tables = [
                    name for name, _ in self.converter.fetch_oracle_tables(oracle_cfg, prefix=prefix)
                ] if prefix else []
            try:
                tables, table_columns, pk_map, table_comments = self.converter.fetch_oracle_schema(
                    oracle_cfg, selected_tables=selected_tables
                )
            except Exception as exc:
                msg = str(exc)
                if "DPY-3010" in msg:
                    raise RuntimeError(
                        "Oracle 版本较低，Thin 模式不支持。请启用 Thick 模式并配置 Instant Client。"
                    ) from exc
                raise

            self.log(f"共读取 {len(tables)} 张表。")

            target_db = options.get("target_db", "clickhouse")
            run_execute = options["execute_on_clickhouse"]
            create_db = options["create_db_if_not_exists"]
            skip_existing = options["skip_existing_tables"]

            sql_output_dir = Path(__file__).with_name("output_sql")
            sql_output_dir.mkdir(parents=True, exist_ok=True)
            target_suffix = "doris" if target_db == "doris" else "ch"
            sql_file = sql_output_dir / f"oracle_to_{target_suffix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql"

            client = None
            if run_execute or skip_existing:
                client = self._open_target_client(target_db, ch_cfg)
                if run_execute and create_db:
                    db_sql = f"CREATE DATABASE IF NOT EXISTS `{ch_cfg['database'].replace('`', '``')}`"
                    self._target_command(client, target_db, db_sql)
                    self.log(f"已确保数据库存在: {ch_cfg['database']}")

            existing_tables = set()
            if skip_existing and client is not None:
                existing_tables = self._target_existing_tables(client, target_db, ch_cfg["database"])
                self.log(f"{target_db} 中已存在表数量: {len(existing_tables)}")

            total = len(tables)
            executed_count = 0
            generated_count = 0
            skipped_count = 0
            with open(sql_file, "w", encoding="utf-8") as f:
                for idx, table in enumerate(tables, start=1):
                    if skip_existing and table.upper() in existing_tables:
                        skipped_count += 1
                        self.log(f"[{idx}/{total}] 跳过已存在表: {table}")
                        self._set_progress(idx, total, table)
                        continue

                    sql = self.converter.build_create_table_sql(
                        ch_database=ch_cfg["database"],
                        table_name=table,
                        columns=table_columns.get(table, []),
                        pk_columns=pk_map.get(table, []),
                        target=target_db,
                        table_comment=table_comments.get(table),
                    )
                    f.write(sql + ";\n\n")
                    generated_count += 1

                    if run_execute and client is not None:
                        self._target_command(client, target_db, sql)
                        executed_count += 1
                        self.log(f"[{idx}/{total}] 已执行建表: {table}")
                    else:
                        self.log(f"[{idx}/{total}] 已生成 SQL: {table}")

                    self._set_progress(idx, total, table)

            if client is not None:
                client.close()

            done_msg = (
                f"转换完成。\nOracle 表总数: {total}\n跳过已存在: {skipped_count}\n"
                f"新生成 SQL: {generated_count}\n已执行建表: {executed_count}\n"
                f"SQL 文件: {sql_file}"
            )
            if not run_execute:
                os.startfile(sql_output_dir)
            self.ui_queue.put(("done", done_msg))
        except Exception as exc:
            self.log(traceback.format_exc())
            self.ui_queue.put(("error", str(exc)))


def main():
    root = tk.Tk()
    style = ttk.Style(root)
    try:
        style.theme_use("clam")
    except tk.TclError:
        pass
    App(root)
    root.mainloop()


def self_check():
    converter = SchemaConverter(lambda _: None)
    columns = [
        ColumnMeta("ID", "NUMBER", None, 10, 0, "N", None),
        ColumnMeta("AMOUNT", "NUMBER", None, 12, 2, "Y", None),
        ColumnMeta("NAME", "NVARCHAR2", 200, None, None, "Y", 100, comment="customer name"),
        ColumnMeta("CODE", "NCHAR", 20, None, None, "Y", 10),
        ColumnMeta("CREATED_AT", "TIMESTAMP(6)", None, None, None, "N", None),
    ]

    ch_sql = converter.build_create_table_sql("DB1", "T_ORDER", columns, ["ID"], "clickhouse")
    assert "`ID` Int64" in ch_sql
    assert "`AMOUNT` Nullable(Decimal(12, 2))" in ch_sql
    assert "ENGINE = MergeTree" in ch_sql

    doris_sql = converter.build_create_table_sql(
        "DB1", "T_ORDER", columns, ["ID"], "doris", "orders table"
    )
    assert "`ID` BIGINT NOT NULL" in doris_sql
    assert "`AMOUNT` DECIMAL(12, 2) NULL" in doris_sql
    assert "`NAME` VARCHAR(300) NULL COMMENT 'customer name'" in doris_sql
    assert "`CODE` CHAR(10) NULL" in doris_sql
    assert "ENGINE=OLAP" in doris_sql
    assert "DUPLICATE KEY(`ID`)\nCOMMENT 'orders table'\nDISTRIBUTED BY" in doris_sql
    assert "DISTRIBUTED BY HASH(`ID`) BUCKETS 10" in doris_sql
    assert "DISTRIBUTED BY HASH(`ID`) BUCKETS 10\nCOMMENT" not in doris_sql


if __name__ == "__main__":
    if "--self-check" in sys.argv:
        self_check()
    else:
        main()
