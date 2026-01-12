import streamlit as st
import sqlite3
import pandas as pd
import re
from enum import Enum
from typing import Optional, Any
from collections import defaultdict, deque
from difflib import get_close_matches
from pathlib import Path
from abc import ABC, abstractmethod

__version__ = "1.0.0"

# ============================================================
# SQL DIALECT & DATABASE ENGINE ARCHITECTURE
# ============================================================

class SQLDialect(Enum):
    SQLITE = "sqlite"
    POSTGRES = "postgres"
    SQLSERVER = "sqlserver"
    MYSQL = "mysql"

class SandboxResult:
    def __init__(self, status: str, df: Optional[pd.DataFrame], message: str):
        self.status = status
        self.df = df
        self.message = message

def q(name: str, d: SQLDialect) -> str:
    """Quote identifier based on SQL dialect"""
    if d == SQLDialect.SQLSERVER:
        return f"[{name}]"
    if d == SQLDialect.MYSQL:
        return f"`{name}`"
    return f'"{name}"'

def render_select(d: SQLDialect, limit: int | None = None) -> str:
    """Render SELECT clause with dialect-specific TOP"""
    if d == SQLDialect.SQLSERVER and limit:
        return f"SELECT TOP {limit}"
    return "SELECT"

def render_limit(d: SQLDialect, limit: int) -> str:
    """Render LIMIT clause (SQL Server uses TOP instead)"""
    if d == SQLDialect.SQLSERVER:
        return ""
    return f"LIMIT {limit}"

def strip_sql_comments(sql: str) -> str:
    """Remove SQL comments from query"""
    sql = re.sub(r'--.*?$', '', sql, flags=re.MULTILINE)
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
    return sql.strip()

def contains_dangerous_sql(sql: str) -> Optional[str]:
    """Check for dangerous SQL keywords"""
    dangerous = ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'ALTER', 'TRUNCATE',
                 'CREATE', 'REPLACE', 'ATTACH', 'DETACH', 'PRAGMA']
    for word in dangerous:
        if re.search(rf'\b{word}\b', sql, re.IGNORECASE):
            return word
    return None

# ============================================================
# DATABASE SCHEMA CLASSES
# ============================================================

class DatabaseSchema:
    """Read-only database schema introspection"""
    
    def __init__(self, tables: list[str], columns: dict[str, list[str]], 
                 relationships: dict[str, list[tuple[str, str, str]]]):
        self.tables = tables
        self.columns = columns
        self.relationships = relationships

    def get_tables(self) -> list[str]:
        return sorted(self.tables)

    def get_columns(self, table: str) -> list[str]:
        return self.columns.get(table, [])

    def find_join_path(self, tables: list[str]) -> Optional[list[tuple[str, str, str, str]]]:
        """Find join path between multiple tables using BFS"""
        if len(tables) < 2:
            return None
        start = tables[0]
        targets = set(tables[1:])
        queue = deque([(start, [])])
        visited = set()
        while queue:
            current, path = queue.popleft()
            if current in targets:
                targets.remove(current)
                if not targets:
                    return path
                queue = deque([(current, path)])
                visited = set()
            if current in visited:
                continue
            visited.add(current)
            for ref_table, local_col, ref_col in self.relationships.get(current, []):
                if ref_table not in visited:
                    queue.append((ref_table, path + [(current, ref_table, local_col, ref_col)]))
            for table in self.tables:
                if table not in visited:
                    for ref_table, local_col, ref_col in self.relationships.get(table, []):
                        if ref_table == current:
                            queue.append((table, path + [(current, table, ref_col, local_col)]))
        return None

# ============================================================
# DATABASE ENGINE ABSTRACTION
# ============================================================

class DatabaseEngine(ABC):
    """Abstract base class for database engines"""
    
    def __init__(self):
        self.conn: Optional[Any] = None
        self.dialect: SQLDialect = SQLDialect.SQLITE
        self.name: str = "Generic"
        self._schema_cache: Optional[DatabaseSchema] = None

    @abstractmethod
    def connect(self, **config) -> tuple[bool, str]:
        pass

    @abstractmethod
    def get_schema(self) -> DatabaseSchema:
        pass

    def refresh_schema(self) -> DatabaseSchema:
        """Force reload of schema cache"""
        self._schema_cache = None
        return self.get_schema()

    def execute_select(self, sql: str) -> SandboxResult:
        """Execute SELECT query with safety checks"""
        if not self.conn:
            return SandboxResult("error", None, "Not connected to database")
        if not sql.strip():
            return SandboxResult("warning", None, "No SQL provided.")
        cleaned = strip_sql_comments(sql)
        if ";" in cleaned.rstrip(";"):
            return SandboxResult("warning", None, 
                "Multiple SQL statements detected. Only single SELECT statements are allowed.")
        dangerous = contains_dangerous_sql(cleaned)
        if dangerous:
            return SandboxResult("warning", None,
                f"This query contains `{dangerous}`. Execution is blocked to prevent data changes.")
        if not cleaned.upper().strip().startswith('SELECT'):
            return SandboxResult("warning", None, "Only SELECT queries are allowed.")
        try:
            df = pd.read_sql_query(cleaned, self.conn)
            if df.empty:
                return SandboxResult("success", df, "Query ran successfully but returned no rows.")
            return SandboxResult("success", df, f"Query executed successfully. Returned {len(df)} row(s).")
        except Exception as e:
            msg = str(e)
            if "no such function" in msg.lower():
                msg += "\n\n💡 This looks like SQL from another dialect."
            elif "syntax error" in msg.lower():
                msg += "\n\n💡 Check commas, joins, or keyword order."
            elif "no such table" in msg.lower():
                msg += "\n\n💡 Table not found. Check spelling and refresh schema."
            return SandboxResult("error", None, msg)

    def test_connection(self) -> tuple[bool, str]:
        """Test if connection is still alive"""
        if not self.conn:
            return False, "No active connection"
        try:
            result = self.execute_select("SELECT 1")
            if result.status == "success":
                return True, "Connection Verified"
            return False, "Connection Test Failed"
        except:
            return False, "Connection Lost"

    def disconnect(self):
        """Close database connection"""
        if self.conn:
            try:
                self.conn.close()
            except:
                pass
            self.conn = None
        self._schema_cache = None

class SQLiteEngine(DatabaseEngine):
    """SQLite database engine"""
    
    def __init__(self):
        super().__init__()
        self.dialect = SQLDialect.SQLITE
        self.name = "SQLite"

    def connect(self, path: str) -> tuple[bool, str]:
        try:
            file_path = Path(path)
            if not file_path.exists():
                return False, f"File not found: {path}"
            if not file_path.is_file():
                return False, f"Not a valid file: {path}"
            if file_path.suffix.lower() not in ['.db', '.sqlite', '.sqlite3', '.db3', '']:
                return False, "Invalid file extension. Expected .db, .sqlite, .sqlite3, or .db3"
            self.disconnect()
            self.conn = sqlite3.connect(str(file_path), check_same_thread=False)
            cur = self.conn.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' LIMIT 1")
            cur.fetchone()
            return True, f"Connected to {file_path.name}"
        except sqlite3.DatabaseError as e:
            return False, f"Invalid SQLite database: {str(e)}"
        except Exception as e:
            return False, f"Connection error: {str(e)}"

    def get_schema(self) -> DatabaseSchema:
        if self._schema_cache is not None:
            return self._schema_cache
        if not self.conn:
            return DatabaseSchema([], {}, {})
        try:
            query = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            tables = [r[0] for r in self.conn.execute(query)]
            columns = {}
            for t in tables:
                try:
                    cur = self.conn.execute(f'PRAGMA table_info("{t}")')
                    columns[t] = [r[1] for r in cur.fetchall()]
                except:
                    columns[t] = []
            relationships = defaultdict(list)
            for t in tables:
                try:
                    cur = self.conn.execute(f'PRAGMA foreign_key_list("{t}")')
                    for r in cur.fetchall():
                        relationships[t].append((r[2], r[3], r[4]))
                except:
                    continue
            self._schema_cache = DatabaseSchema(tables, columns, dict(relationships))
            return self._schema_cache
        except:
            return DatabaseSchema([], {}, {})

class PostgreSQLEngine(DatabaseEngine):
    """PostgreSQL database engine"""
    
    def __init__(self):
        super().__init__()
        self.dialect = SQLDialect.POSTGRES
        self.name = "PostgreSQL"

    def connect(self, host: str, port: int, database: str, user: str, password: str) -> tuple[bool, str]:
        try:
            import psycopg2
            self.disconnect()
            self.conn = psycopg2.connect(host=host, port=port, database=database, 
                                        user=user, password=password, connect_timeout=10)
            return True, f"Connected to PostgreSQL: {database}@{host}"
        except ImportError:
            return False, "psycopg2 library not installed. Run: pip install psycopg2-binary"
        except Exception as e:
            return False, f"PostgreSQL connection failed: {str(e)}"

    def get_schema(self) -> DatabaseSchema:
        if self._schema_cache is not None:
            return self._schema_cache
        if not self.conn:
            return DatabaseSchema([], {}, {})
        try:
            cur = self.conn.cursor()
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = [r[0] for r in cur.fetchall()]
            columns = {}
            for t in tables:
                cur.execute("""SELECT column_name FROM information_schema.columns 
                            WHERE table_name = %s AND table_schema = 'public' 
                            ORDER BY ordinal_position""", (t,))
                columns[t] = [r[0] for r in cur.fetchall()]
            relationships = defaultdict(list)
            for t in tables:
                cur.execute("""SELECT kcu.column_name, ccu.table_name AS foreign_table_name,
                            ccu.column_name AS foreign_column_name
                            FROM information_schema.key_column_usage AS kcu
                            JOIN information_schema.constraint_column_usage AS ccu
                            ON kcu.constraint_name = ccu.constraint_name
                            WHERE kcu.table_name = %s AND kcu.table_schema = 'public'
                            AND kcu.constraint_name IN (SELECT constraint_name 
                            FROM information_schema.table_constraints 
                            WHERE constraint_type = 'FOREIGN KEY')""", (t,))
                for r in cur.fetchall():
                    relationships[t].append((r[1], r[0], r[2]))
            cur.close()
            self._schema_cache = DatabaseSchema(tables, columns, dict(relationships))
            return self._schema_cache
        except:
            return DatabaseSchema([], {}, {})

class MySQLEngine(DatabaseEngine):
    """MySQL database engine"""
    
    def __init__(self):
        super().__init__()
        self.dialect = SQLDialect.MYSQL
        self.name = "MySQL"

    def connect(self, host: str, port: int, database: str, user: str, password: str) -> tuple[bool, str]:
        try:
            import mysql.connector
            self.disconnect()
            self.conn = mysql.connector.connect(host=host, port=port, database=database,
                                               user=user, password=password, connection_timeout=10)
            return True, f"Connected to MySQL: {database}@{host}"
        except ImportError:
            return False, "mysql-connector-python not installed. Run: pip install mysql-connector-python"
        except Exception as e:
            return False, f"MySQL connection failed: {str(e)}"

    def get_schema(self) -> DatabaseSchema:
        if self._schema_cache is not None:
            return self._schema_cache
        if not self.conn:
            return DatabaseSchema([], {}, {})
        try:
            cur = self.conn.cursor()
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()")
            tables = [r[0] for r in cur.fetchall()]
            columns = {}
            for t in tables:
                cur.execute("""SELECT column_name FROM information_schema.columns 
                            WHERE table_name = %s AND table_schema = DATABASE() 
                            ORDER BY ordinal_position""", (t,))
                columns[t] = [r[0] for r in cur.fetchall()]
            relationships = defaultdict(list)
            for t in tables:
                cur.execute("""SELECT kcu.column_name, kcu.referenced_table_name,
                            kcu.referenced_column_name
                            FROM information_schema.key_column_usage AS kcu
                            WHERE kcu.table_name = %s AND kcu.table_schema = DATABASE()
                            AND kcu.referenced_table_name IS NOT NULL""", (t,))
                for r in cur.fetchall():
                    relationships[t].append((r[1], r[0], r[2]))
            cur.close()
            self._schema_cache = DatabaseSchema(tables, columns, dict(relationships))
            return self._schema_cache
        except:
            return DatabaseSchema([], {}, {})

class SQLServerEngine(DatabaseEngine):
    """SQL Server database engine"""
    
    def __init__(self):
        super().__init__()
        self.dialect = SQLDialect.SQLSERVER
        self.name = "SQL Server"

    def connect(self, server: str, database: str, user: str, password: str) -> tuple[bool, str]:
        try:
            import pyodbc
            drivers = [d for d in pyodbc.drivers() if 'SQL Server' in d]
            if not drivers:
                return False, "No SQL Server ODBC driver found. Install ODBC Driver 17 or 18 for SQL Server"
            self.disconnect()
            driver = drivers[0]
            conn_str = (f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};"
                       f"UID={user};PWD={password};TrustServerCertificate=yes;")
            self.conn = pyodbc.connect(conn_str, timeout=10)
            return True, f"Connected to SQL Server: {database}@{server}"
        except ImportError:
            return False, "pyodbc library not installed. Run: pip install pyodbc"
        except Exception as e:
            return False, f"SQL Server connection failed: {str(e)}"

    def get_schema(self) -> DatabaseSchema:
        if self._schema_cache is not None:
            return self._schema_cache
        if not self.conn:
            return DatabaseSchema([], {}, {})
        try:
            cur = self.conn.cursor()
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'")
            tables = [r[0] for r in cur.fetchall()]
            columns = {}
            for t in tables:
                cur.execute("""SELECT column_name FROM information_schema.columns 
                            WHERE table_name = ? ORDER BY ordinal_position""", (t,))
                columns[t] = [r[0] for r in cur.fetchall()]
            relationships = defaultdict(list)
            for t in tables:
                cur.execute("""SELECT COL_NAME(fc.parent_object_id, fc.parent_column_id) AS column_name,
                            OBJECT_NAME(fc.referenced_object_id) AS referenced_table,
                            COL_NAME(fc.referenced_object_id, fc.referenced_column_id) AS referenced_column
                            FROM sys.foreign_key_columns AS fc
                            WHERE OBJECT_NAME(fc.parent_object_id) = ?""", (t,))
                for r in cur.fetchall():
                    relationships[t].append((r[1], r[0], r[2]))
            cur.close()
            self._schema_cache = DatabaseSchema(tables, columns, dict(relationships))
            return self._schema_cache
        except:
            return DatabaseSchema([], {}, {})

# ============================================================
# ENGINE MANAGEMENT
# ============================================================

def get_engine() -> DatabaseEngine:
    """Get or create database engine from session state"""
    if "engine" not in st.session_state:
        engine = SQLiteEngine()
        success, msg = engine.connect("northwind.db")
        if success:
            st.session_state["engine"] = engine
            st.session_state["db_name"] = "northwind.db"
            st.session_state["connection_time"] = pd.Timestamp.now()
        else:
            st.session_state["engine"] = engine
            st.session_state["db_name"] = "Not connected"
    return st.session_state["engine"]

# ============================================================
# STREAMLIT APP
# ============================================================

st.set_page_config(page_title=f"SQL Schema Workbench v{__version__}", layout="wide",
                   initial_sidebar_state="expanded")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&display=swap');
* { font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; }
.block-container { padding: 3rem 2rem; max-width: 1400px; }
:root {
    --bg-primary: #262730; --bg-secondary: #1a1b26; --bg-tertiary: #2d2e3a;
    --border-primary: #404152; --border-secondary: #353642;
    --text-primary: #e8eaed; --text-secondary: #c4c7cc; --text-tertiary: #9aa0a6;
    --accent-primary: #4a9eff; --accent-hover: #5ba8ff;
    --success: #4caf50; --warning: #ffb84d; --error: #ff6b6b;
}
h1 { font-size: 1.7rem; font-weight: 600; color: var(--text-primary); margin-bottom: 0.5rem; letter-spacing: -0.03em; }
h2 { font-size: 1.3rem; font-weight: 600; color: var(--text-primary); margin: 2rem 0 1rem 0; 
     padding-bottom: 0.5rem; border-bottom: 1px solid var(--border-secondary); letter-spacing: -0.02em; }
h3 { font-size: 1.15rem; font-weight: 600; color: var(--text-primary); margin: 1.5rem 0 0.75rem 0; }
p, .stMarkdown { color: var(--text-secondary); line-height: 1.6; font-size: 0.95rem; }
.stTabs { background-color: var(--bg-secondary); border-radius: 8px; padding: 0; margin: 1rem 0; }
.stTabs [data-baseweb="tab-list"] { gap: 0; background-color: var(--bg-secondary); 
    border-bottom: 1px solid var(--border-primary); padding: 0 1rem; border-radius: 8px 8px 0 0; }
.stTabs [data-baseweb="tab"] { height: 48px; padding: 0 1.5rem; background-color: transparent;
    border: none; color: var(--text-tertiary); font-weight: 500; font-size: 0.875rem; }
.stTabs [data-baseweb="tab"]:hover { background-color: rgba(255, 255, 255, 0.05); color: var(--text-primary); }
.stTabs [aria-selected="true"] { color: var(--accent-primary); background-color: var(--bg-primary); 
    border-bottom: 2px solid var(--accent-primary); }
.stTabs [data-baseweb="tab-panel"] { background-color: var(--bg-primary); padding: 2rem; border-radius: 0 0 8px 8px; }
pre { background-color: #1e1e1e !important; border: 1px solid #333 !important; border-radius: 6px !important;
    padding: 1rem !important; font-family: 'Consolas', 'Monaco', monospace !important; font-size: 0.875rem !important;
    line-height: 1.6 !important; overflow-x: auto !important; color: #d4d4d4 !important; }
code { background-color: #2d2e3a; padding: 0.2em 0.4em; border-radius: 4px;
    font-family: 'Consolas', 'Monaco', monospace; font-size: 0.875rem; color: #ff6b6b; 
    border: 1px solid var(--border-secondary); }
.stTextInput > div > div > input, .stTextArea textarea {
    background-color: var(--bg-secondary); border: 1px solid var(--border-primary); border-radius: 6px;
    padding: 0.75rem; font-family: 'Consolas', 'Monaco', monospace; font-size: 0.9rem;
    color: var(--text-primary); transition: border-color 0.2s, box-shadow 0.2s; }
.stTextInput > div > div > input:focus, .stTextArea textarea:focus {
    border-color: var(--accent-primary); box-shadow: 0 0 0 3px rgba(74, 158, 255, 0.2); outline: none; }
.stButton > button { background-color: var(--bg-tertiary); color: var(--text-primary);
    border: 1px solid var(--border-primary); border-radius: 6px; font-weight: 500;
    padding: 0.5rem 1rem; font-size: 0.875rem; transition: all 0.15s; box-shadow: 0 1px 0 rgba(0, 0, 0, 0.2); }
.stButton > button:hover { background-color: var(--bg-primary); border-color: var(--border-primary);
    box-shadow: 0 1px 0 rgba(0, 0, 0, 0.2), 0 0 0 3px rgba(74, 158, 255, 0.2); }
.stButton > button[kind="primary"] { background-color: var(--accent-primary); color: white; 
    border-color: var(--accent-primary); }
.stButton > button[kind="primary"]:hover { background-color: var(--accent-hover); border-color: var(--accent-hover); }
.streamlit-expanderHeader { background-color: var(--bg-tertiary); border: 1px solid var(--border-secondary);
    border-radius: 6px; padding: 0.75rem 1rem; font-size: 0.9rem; font-weight: 500;
    color: var(--text-primary); transition: background-color 0.15s; }
.streamlit-expanderHeader:hover { background-color: var(--bg-primary); }
details[open] .streamlit-expanderHeader { border-radius: 6px 6px 0 0; border-bottom-color: transparent; }
.streamlit-expanderContent { border: 1px solid var(--border-secondary); border-top: none;
    border-radius: 0 0 6px 6px; padding: 1rem; background-color: var(--bg-secondary); }
[data-testid="stMetricValue"] { font-size: 1.75rem; font-weight: 600; color: var(--text-primary); }
[data-testid="stMetricLabel"] { font-size: 0.875rem; color: var(--text-tertiary); font-weight: 500; }
.dataframe { font-size: 0.875rem; border: 1px solid var(--border-primary) !important;
    border-radius: 6px !important; color: var(--text-primary) !important; }
.dataframe thead th { background-color: var(--bg-tertiary) !important; color: var(--text-primary) !important;
    font-weight: 600 !important; border-bottom: 2px solid var(--border-primary) !important; }
.dataframe tbody td { color: var(--text-secondary) !important; }
.stAlert { border-radius: 6px; border: 1px solid; padding: 0.75rem 1rem; font-size: 0.875rem; }
.stSuccess { background-color: rgba(76, 175, 80, 0.15); border-color: var(--success); color: #81c784; }
.stError { background-color: rgba(255, 107, 107, 0.15); border-color: var(--error); color: #ff8a8a; }
.stWarning { background-color: rgba(255, 184, 77, 0.15); border-color: var(--warning); color: #ffca80; }
.stInfo { background-color: rgba(74, 158, 255, 0.15); border-color: var(--accent-primary); color: #70b4ff; }
[data-testid="stSidebar"] { background-color: var(--bg-secondary); border-right: 1px solid var(--border-primary); padding: 2rem 1rem; }
[data-testid="stSidebar"] h2 { font-size: 1.1rem; border-bottom: 1px solid var(--border-primary);
    padding-bottom: 0.5rem; margin-bottom: 1rem; color: var(--text-primary); }
hr { margin: 2rem 0; border: none; border-top: 1px solid var(--border-secondary); }
.caption, [data-testid="stCaptionContainer"] { color: var(--text-tertiary); font-size: 0.875rem; line-height: 1.5; }
</style>
""", unsafe_allow_html=True)

engine = get_engine()
schema = engine.get_schema()

if "sql_dialect" not in st.session_state:
    st.session_state["sql_dialect"] = engine.dialect

st.title("SQL Schema Workbench")
st.caption(f"A read-only SQL workbench for safe database exploration • v{__version__}")

with st.sidebar:
    st.header("Settings")
    st.selectbox("SQL Dialect", options=list(SQLDialect), format_func=lambda d: d.name,
                 key="sql_dialect", help="Choose SQL dialect for query generation")
    st.divider()
    st.caption("**Active Database:**")
    col_db1, col_db2 = st.columns([3, 1])
    with col_db1:
        st.code(st.session_state.get('db_name', 'Not connected'))
    with col_db2:
        if st.button("🔄", help="Refresh schema", key="refresh_schema"):
            with st.spinner("Refreshing..."):
                schema = engine.refresh_schema()
                st.success("✓")
                st.rerun()
    st.caption(f"**Engine:** {engine.name}")
    is_alive, status_msg = engine.test_connection()
    if is_alive:
        st.success(f"✓ {status_msg}")
    else:
        st.error(f"✗ {status_msg}")
    if "connection_time" in st.session_state:
        conn_time = st.session_state["connection_time"]
        duration = pd.Timestamp.now() - conn_time
        st.caption(f"Connected: {duration.components.hours}h {duration.components.minutes}m ago")

d = st.session_state["sql_dialect"]

tab_db, tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "🔌 Database", "⚡ SQL Tester", "📊 Schema Explorer",
    "🔍 Logic by Example", "🗃️ Query Builder", "🔧 RegEx Tools"
])

# ============================================================
# DATABASE CONNECTION TAB
# ============================================================

with tab_db:
    st.header("Database Connection")
    st.markdown("Connect to local SQLite files or remote databases. All queries are read-only by design.")
    st.markdown("### Current Connection")
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        st.code(st.session_state.get('db_name', 'Not connected'))
    with col2:
        st.metric("Engine", engine.name)
    with col3:
        if schema and schema.get_tables():
            st.metric("Tables", len(schema.get_tables()))
    col_test1, col_test2 = st.columns([1, 3])
    with col_test1:
        if st.button("Test Connection", key="test_conn_main"):
            is_alive, msg = engine.test_connection()
            if is_alive:
                st.success(msg)
            else:
                st.error(msg)
    st.divider()
    st.markdown("### Connect to New Database")
    connection_type = st.selectbox("Database Type", options=["SQLite", "PostgreSQL", "MySQL", "SQL Server"],
                                   help="Choose your database type")
    
    if connection_type == "SQLite":
        st.markdown("#### SQLite File Connection")
        db_path = st.text_input("Database file path", placeholder="northwind.db or /path/to/database.db",
                                help="Enter path to .db, .sqlite, or .sqlite3 file")
        col1, col2 = st.columns([1, 1])
        with col1:
            if st.button("Connect", type="primary", key="connect_sqlite"):
                if not db_path.strip():
                    st.warning("Please enter a valid database path.")
                else:
                    new_engine = SQLiteEngine()
                    success, message = new_engine.connect(db_path.strip())
                    if success:
                        if "engine" in st.session_state:
                            st.session_state["engine"].disconnect()
                        st.session_state["engine"] = new_engine
                        st.session_state["db_name"] = db_path.strip()
                        st.session_state["connection_time"] = pd.Timestamp.now()
                        st.success(message)
                        st.rerun()
                    else:
                        st.error(message)
        with col2:
            if st.button("Reset to Northwind", key="reset_northwind"):
                new_engine = SQLiteEngine()
                success, message = new_engine.connect("northwind.db")
                if success:
                    if "engine" in st.session_state:
                        st.session_state["engine"].disconnect()
                    st.session_state["engine"] = new_engine
                    st.session_state["db_name"] = "northwind.db"
                    st.session_state["connection_time"] = pd.Timestamp.now()
                    st.success("Connected to Northwind sample database")
                    st.rerun()
    
    elif connection_type == "PostgreSQL":
        st.markdown("#### PostgreSQL Connection")
        col1, col2 = st.columns([2, 1])
        with col1:
            pg_host = st.text_input("Host", value="localhost", key="pg_host")
            pg_database = st.text_input("Database", placeholder="mydb", key="pg_db")
            pg_user = st.text_input("Username", placeholder="postgres", key="pg_user")
        with col2:
            pg_port = st.number_input("Port", value=5432, min_value=1, max_value=65535, key="pg_port")
            pg_password = st.text_input("Password", type="password", key="pg_pass")
        if st.button("Connect to PostgreSQL", type="primary", key="connect_pg"):
            if not all([pg_host, pg_database, pg_user, pg_password]):
                st.warning("Please fill in all required fields.")
            else:
                new_engine = PostgreSQLEngine()
                success, message = new_engine.connect(pg_host, pg_port, pg_database, pg_user, pg_password)
                if success:
                    if "engine" in st.session_state:
                        st.session_state["engine"].disconnect()
                    st.session_state["engine"] = new_engine
                    st.session_state["db_name"] = f"{pg_database}@{pg_host}:{pg_port}"
                    st.session_state["connection_time"] = pd.Timestamp.now()
                    st.success(message)
                    st.rerun()
                else:
                    st.error(message)
    
    elif connection_type == "MySQL":
        st.markdown("#### MySQL Connection")
        col1, col2 = st.columns([2, 1])
        with col1:
            my_host = st.text_input("Host", value="localhost", key="my_host")
            my_database = st.text_input("Database", placeholder="mydb", key="my_db")
            my_user = st.text_input("Username", placeholder="root", key="my_user")
        with col2:
            my_port = st.number_input("Port", value=3306, min_value=1, max_value=65535, key="my_port")
            my_password = st.text_input("Password", type="password", key="my_pass")
        if st.button("Connect to MySQL", type="primary", key="connect_my"):
            if not all([my_host, my_database, my_user, my_password]):
                st.warning("Please fill in all required fields.")
            else:
                new_engine = MySQLEngine()
                success, message = new_engine.connect(my_host, my_port, my_database, my_user, my_password)
                if success:
                    if "engine" in st.session_state:
                        st.session_state["engine"].disconnect()
                    st.session_state["engine"] = new_engine
                    st.session_state["db_name"] = f"{my_database}@{my_host}:{my_port}"
                    st.session_state["connection_time"] = pd.Timestamp.now()
                    st.success(message)
                    st.rerun()
                else:
                    st.error(message)
    
    elif connection_type == "SQL Server":
        st.markdown("#### SQL Server Connection")
        col1, col2 = st.columns([2, 1])
        with col1:
            ss_server = st.text_input("Server", placeholder="localhost or server.domain.com\\INSTANCE",
                                     key="ss_server", help="Server name or IP. Can include instance: SERVER\\INSTANCE")
            ss_database = st.text_input("Database", placeholder="master", key="ss_db")
        with col2:
            ss_user = st.text_input("Username", placeholder="sa", key="ss_user")
            ss_password = st.text_input("Password", type="password", key="ss_pass")
        if st.button("Connect to SQL Server", type="primary", key="connect_ss"):
            if not all([ss_server, ss_database, ss_user, ss_password]):
                st.warning("Please fill in all required fields.")
            else:
                new_engine = SQLServerEngine()
                success, message = new_engine.connect(ss_server, ss_database, ss_user, ss_password)
                if success:
                    if "engine" in st.session_state:
                        st.session_state["engine"].disconnect()
                    st.session_state["engine"] = new_engine
                    st.session_state["db_name"] = f"{ss_database}@{ss_server}"
                    st.session_state["connection_time"] = pd.Timestamp.now()
                    st.success(message)
                    st.rerun()
                else:
                    st.error(message)
    
    st.divider()
    with st.expander("💡 Connection Examples & Tips"):
        st.markdown("""
        #### SQLite
        - **Local file**: `northwind.db` or `./data/mydb.sqlite`
        - **Absolute path**: `/home/user/databases/app.db`
        
        #### PostgreSQL
        - **Local**: host=`localhost`, port=`5432`, database=`mydb`
        - **Remote**: host=`db.company.com`, port=`5432`
        - **Cloud (AWS RDS)**: host=`mydb.abc123.us-east-1.rds.amazonaws.com`
        
        #### MySQL
        - **Local**: host=`localhost` or `127.0.0.1`, port=`3306`
        - **Remote**: host=`192.168.1.50`, database=`sales`
        
        #### SQL Server
        - **Local**: server=`localhost` or `(local)`
        - **Named instance**: server=`SERVER\\SQLEXPRESS`
        - **Remote**: server=`sql.company.com`
        - **Azure SQL**: server=`myserver.database.windows.net`
        
        #### Required Libraries
        Install these if connecting to network databases:
        ```bash
        pip install psycopg2-binary     # PostgreSQL
        pip install mysql-connector-python  # MySQL
        pip install pyodbc              # SQL Server
        ```
        """)
    
    with st.expander("🔒 Security & Usage Information"):
        st.markdown("""
        #### Security Features
        - ✅ Only SELECT queries permitted
        - ✅ Data modification blocked (INSERT, UPDATE, DELETE, DROP)
        - ✅ No PRAGMA or ATTACH commands
        - ✅ Multi-statement queries blocked
        - ✅ 10-second connection timeout for network databases
        
        #### Performance Notes
        - Schema cached per database connection
        - Query results limited to prevent memory issues
        
        #### Credentials
        ⚠️ **Security Warning**: Credentials are stored in session memory only and are cleared when you close the browser.
        """)

# ============================================================
# SQL TESTER TAB
# ============================================================

with tab1:
    st.header("SQL Tester")
    st.markdown("Write and test SELECT queries safely. All modification queries are blocked.")
    default_sql = ""
    if 'sql_to_test' in st.session_state:
        default_sql = st.session_state['sql_to_test']
        st.success("✓ Query loaded from Query Builder")
        del st.session_state['sql_to_test']
    sql = st.text_area("Write SQL:", value=default_sql, height=220, placeholder="SELECT * FROM table_name LIMIT 10")
    col1, col2 = st.columns([1, 5])
    with col1:
        run_button = st.button("Run SQL", type="primary")
    with col2:
        if sql.strip():
            st.caption(f"Query length: {len(sql)} characters")
    if run_button:
        if not schema or not schema.get_tables():
            st.error("No database schema loaded. Please connect to a database first.")
        else:
            result = engine.execute_select(sql)
            if result.status == "success":
                st.success(result.message)
                if result.df is not None and not result.df.empty:
                    st.dataframe(result.df, use_container_width=True)
            elif result.status == "warning":
                st.warning(result.message)
            else:
                st.error(result.message)

# ============================================================
# SCHEMA EXPLORER TAB
# ============================================================

with tab2:
    st.header("Schema Explorer")
    if not schema or not schema.get_tables():
        st.warning("No tables found in the current database schema.")
    else:
        st.caption(f"Exploring {len(schema.get_tables())} tables")
        table = st.selectbox("Choose a table:", schema.get_tables())
        if table:
            cols = schema.get_columns(table)
            col1, col2 = st.columns([1, 1])
            with col1:
                st.metric("Columns", len(cols))
            with col2:
                fk_count = len(schema.relationships.get(table, []))
                st.metric("Foreign Keys", fk_count)
            st.subheader("Columns")
            st.write(", ".join(f"`{c}`" for c in cols))
            st.subheader("Sample Data")
            try:
                quoted_table = q(table, engine.dialect)
                preview_sql = f'SELECT * FROM {quoted_table} LIMIT 10;'
                result = engine.execute_select(preview_sql)
                if result.status == "success" and result.df is not None:
                    st.dataframe(result.df, use_container_width=True)
                else:
                    st.error(f"Failed to load sample data: {result.message}")
            except Exception as e:
                st.error(f"Failed to load sample data: {e}")

# ============================================================
# LOGIC BY EXAMPLE TAB
# ============================================================

with tab3:
    st.header("Logic by Example")
    st.caption("Find which tables and columns contain specific values")
    sample = st.text_area("Paste sample values (IDs, names, etc):", height=120,
                         placeholder="Enter values separated by newlines, commas, or semicolons")
    if st.button("Search", type="primary"):
        if not schema or not schema.get_tables():
            st.warning("No tables available in the current database.")
        elif not sample.strip():
            st.warning("Please enter at least one value to search for.")
        else:
            values = [v.strip() for v in re.split(r'[\n,;|]', sample) if v.strip()]
            st.info(f"Searching for {len(values)} value(s) across {len(schema.get_tables())} tables. This may take a moment")
            results = []
            for table in schema.get_tables():
                try:
                    quoted_table = q(table, engine.dialect)
                    search_sql = f'SELECT * FROM {quoted_table} LIMIT 1000'
                    result = engine.execute_select(search_sql)
                    if result.status == "success" and result.df is not None:
                        df = result.df
                        for col in df.columns:
                            for v in values:
                                if df[col].astype(str).str.contains(v, case=False, regex=False, na=False).any():
                                    results.append((table, col, v))
                except Exception:
                    continue
            if not results:
                st.info("No matches found.")
            else:
                st.success(f"Found {len(results)} match(es)")
                for t, c, v in results:
                    st.write(f"✓ **{t}.{c}** contains `{v}`")

# ============================================================
# AUTO QUERY BUILDER TAB
# ============================================================

with tab4:
    st.header("Auto Query Builder")
    st.caption("Describe what you're trying to find in plain terms. The tool will identify relevant tables and draft SQL.")
    if not schema or not schema.get_tables():
        st.warning("No database schema available. Please connect to a database first.")
    else:
        term_to_columns = defaultdict(set)
        intent_input = st.text_input("What are you looking for? (comma-separated terms)",
                                    placeholder="products, orders, customers, discontinued",
                                    help="Enter database concepts you want to query")
        custom_synonyms = {
            'sold': ['orders', 'orderdetails', 'quantity', 'sales'],
            'sales': ['orders', 'orderdetails', 'unitprice', 'quantity'],
            'revenue': ['unitprice', 'quantity', 'discount', 'orders'],
            'bought': ['orders', 'orderdetails'], 'purchased': ['orders', 'orderdetails'],
            'income': ['unitprice', 'orders'], 'amount': ['quantity', 'unitprice'],
            'buyer': ['customers', 'customerid'], 'customer': ['customers', 'customerid'],
            'worker': ['employees', 'employeeid'], 'employee': ['employees', 'employeeid'],
            'staff': ['employees', 'employeeid'], 'item': ['products', 'productid'],
            'product': ['products', 'productid'], 'goods': ['products', 'productid'],
            'supplier': ['suppliers', 'supplierid'], 'vendor': ['suppliers', 'supplierid'],
            'shipper': ['shippers', 'shipperid'], 'carrier': ['shippers', 'shipperid'],
            'category': ['categories', 'categoryid'], 'type': ['categories', 'categoryid'],
            'discontinued': ['discontinued'], 'freight': ['freight'], 'shipping': ['freight', 'shippers'],
        }
        
        def discover_term(term: str) -> tuple[set[str], list[str]]:
            term_lower = term.lower()
            matches: set[str] = set()
            explanations: list[str] = []
            search_space: list[str] = []
            for table in schema.get_tables():
                search_space.append(table)
                for column in schema.get_columns(table):
                    search_space.append(f"{table}.{column}")
            for name in search_space:
                if "." in name:
                    table, column = name.split(".", 1)
                    if term_lower in column.lower():
                        matches.add(name)
                        explanations.append(f"✓ Direct match: `{name}`")
                else:
                    if term_lower in name.lower():
                        matches.add(name)
                        explanations.append(f"✓ Direct match: `{name}`")
            if not matches and term_lower in custom_synonyms:
                for synonym in custom_synonyms[term_lower]:
                    synonym_lower = synonym.lower()
                    for name in search_space:
                        if "." in name:
                            table, column = name.split(".", 1)
                            if synonym_lower in column.lower():
                                matches.add(name)
                                explanations.append(f"Synonym '{synonym}' matched: `{name}`")
                        else:
                            if synonym_lower in name.lower():
                                matches.add(name)
                                explanations.append(f"Synonym '{synonym}' matched: `{name}`")
            if not matches:
                candidates = [name.lower() for name in search_space]
                suggestions = get_close_matches(term_lower, candidates, n=3, cutoff=0.6)
                if suggestions:
                    explanations.append(f"Possible matches: {', '.join(f'`{s}`' for s in suggestions)}")
            return matches, explanations
        
        discovered_tables = set()
        discovered_columns = defaultdict(set)
        unmatched_terms = []
        
        if intent_input:
            st.markdown("### Step 1 – Discovery Results")
            terms = [t.strip() for t in intent_input.split(",") if t.strip()]
            for term in terms:
                matches, explanations = discover_term(term)
                term_to_columns[term] = matches
                if matches:
                    header = f"🟢 {term.upper()} ({len(matches)} matches)"
                else:
                    header = f"🔴 {term.upper()} (No match)"
                with st.expander(header, expanded=False):
                    if matches:
                        for m in sorted(matches):
                            st.code(m)
                            if "." in m:
                                table, col = m.split(".", 1)
                                discovered_tables.add(table)
                                discovered_columns[table].add(col)
                            else:
                                discovered_tables.add(m)
                        for exp in explanations:
                            st.caption(exp)
                    else:
                        unmatched_terms.append(term)
                        st.error("No matches found. Try Schema Explorer or a synonym.")
                        for exp in explanations:
                            st.caption(exp)
        
        sql_preview = None
        
        if discovered_tables:
            st.markdown("### Step 2 – Table Relationships")
            final_tables = set()
            for term, matches in term_to_columns.items():
                table_matches = [m for m in matches if "." not in m]
                if table_matches:
                    final_tables.add(table_matches[0])
                else:
                    for m in matches:
                        if "." in m:
                            final_tables.add(m.split(".")[0])
            tables_to_join = list(final_tables) if final_tables else list(discovered_tables)
            st.write(f"**Target tables:** {', '.join(f'`{t}`' for t in sorted(tables_to_join))}")
            join_path = None
            if len(tables_to_join) > 1:
                join_path = schema.find_join_path(tables_to_join)
                if join_path:
                    st.success(f"✓ Found relationship path connecting {len(tables_to_join)} tables")
                    with st.expander("View join path"):
                        for l, r, lc, rc in join_path:
                            st.write(f"`{l}.{lc}` → `{r}.{rc}`")
                else:
                    st.warning("These tables don't appear to be directly related. Try simplifying your search or use Schema Explorer.")
            st.markdown("### Step 3 – Generated SQL")
            select_parts = []
            if len(tables_to_join) == 1:
                sql_preview = f'{render_select(d)}\n  *\nFROM {q(tables_to_join[0], d)}\n{render_limit(d, 10)}'
            elif join_path:
                for t in discovered_tables:
                    cols = discovered_columns.get(t, set())
                    for c in cols:
                        select_parts.append(f'{q(t, d)}.{q(c, d)}')
                if not select_parts:
                    select_parts = ["*"]
                sql_preview = f"{render_select(d, 10 if d == SQLDialect.SQLSERVER else None)}\n  "
                sql_preview += ",\n  ".join(select_parts)
                sql_preview += f'\nFROM {q(tables_to_join[0], d)}\n'
                for left_tab, right_tab, left_col, right_col in join_path:
                    sql_preview += (f'JOIN {q(right_tab, d)} ON '
                                  f'{q(left_tab, d)}.{q(left_col, d)} = {q(right_tab, d)}.{q(right_col, d)}\n')
                sql_preview += render_limit(d, 10)
            else:
                sql_preview = f"-- Unable to determine relationships\n-- Tables: {', '.join(tables_to_join)}"
            st.code(sql_preview, language="sql")
            if sql_preview and not sql_preview.startswith('--'):
                st.markdown("### Step 4 – Refine Query")
                with st.expander("Add WHERE filters", expanded=False):
                    suggestions = sorted({c for cols in term_to_columns.values() for c in cols if "." in c})[:5]
                    where_placeholder = "Enter filter conditions (without WHERE keyword)\n\n"
                    if suggestions:
                        where_placeholder += "Suggested filters:\n"
                        for s in suggestions:
                            where_placeholder += f"- {s}\n"
                    where_placeholder += "\nExample:\nProducts.Discontinued = 1\nAND Orders.OrderDate >= '1997-01-01'"
                    where_conditions = st.text_area("Filter conditions:", placeholder=where_placeholder, height=140)
                    base_query = sql_preview
                    if d == SQLDialect.SQLSERVER:
                        base_query = base_query.replace(f"TOP 10", "TOP 100")
                    else:
                        base_query = base_query.replace('LIMIT 10', '')
                    if where_conditions.strip():
                        conditions = where_conditions.strip()
                        if conditions.upper().startswith('WHERE'):
                            conditions = conditions[5:].strip()
                        base_query += f"\nWHERE {conditions}\n"
                    if d != SQLDialect.SQLSERVER:
                        base_query += "LIMIT 100"
                    refined_query = base_query
                    if where_conditions.strip():
                        st.info("✓ Filters will be applied")
                    if st.button("Send to SQL Tester", type="primary"):
                        st.session_state['sql_to_test'] = refined_query
                        st.success("✓ Query ready! Switch to SQL Tester tab to run it.")
        if unmatched_terms:
            with st.expander("⚠️ Unmatched Terms"):
                st.write("These terms were not found:")
                for t in unmatched_terms:
                    st.code(t)

# ============================================================
# REGEX TOOLS TAB
# ============================================================

with tab5:
    st.header("🔧 RegEx Tools")
    st.caption("Test patterns, understand syntax, and derive patterns from examples")
    regex_tab1, regex_tab2, regex_tab3 = st.tabs(["Tester", "Explainer", "Pattern Generator"])
    
    with regex_tab1:
        st.subheader("Test a Pattern")
        pattern = st.text_input("RegEx Pattern", value=r"\$\d+", help="Enter a regular expression pattern to test")
        test_string = st.text_area("Text to search", value="I have £10, $3, $50, and £5.", height=100)
        if pattern:
            try:
                matches = list(re.finditer(pattern, test_string))
                if matches:
                    st.success(f"✓ {len(matches)} match(es) found")
                    for i, m in enumerate(matches, 1):
                        st.code(f"Match {i}: '{m.group()}' at position {m.start()}-{m.end()}")
                else:
                    st.info("No matches found.")
            except re.error as e:
                st.error(f"Invalid RegEx pattern: {e}")
    
    with regex_tab2:
        st.subheader("Pattern Explainer")
        st.caption("Understand what a RegEx pattern does")
        explain_input = st.text_input("Enter a RegEx pattern", value=r"(SKU|PART)\s*(\d+)",
                                     help="Paste a pattern to see what each part means")
        if explain_input:
            st.markdown("**Pattern breakdown:**")
            tokens = {
                r"\d": "A single digit (0-9)", r"\d+": "One or more digits",
                r"\w": "Word character (letter, digit, underscore)", r"\w+": "One or more word characters",
                r"\s": "Whitespace character", r"\s*": "Zero or more whitespace", r"\s+": "One or more whitespace",
                r"[a-zA-Z]+": "One or more letters", r"[0-9]+": "One or more digits",
                r"|": "OR operator (alternation)", r"\b": "Word boundary",
                r"^": "Start of string", r"$": "End of string", r".": "Any character",
                r"(": "Start capture group", r")": "End capture group",
                r"*": "Zero or more of previous", r"+": "One or more of previous", r"?": "Zero or one of previous",
                r"\.": "Literal dot", r"\\": "Escape character",
            }
            found_tokens = []
            for symbol, desc in tokens.items():
                if symbol in explain_input:
                    found_tokens.append((symbol, desc))
            if found_tokens:
                for symbol, desc in found_tokens:
                    st.markdown(f"- `{symbol}` → {desc}")
            literals = re.sub(r'\\[dwsWDSbB\.\(\)\[\]\{\}\|\*\+\?]|[\(\)\|\*\+\?\.\[\]\{\}]', '', explain_input)
            if literals.strip():
                st.markdown(f"- **Literals:** `{literals.strip()}` (matches exact text)")
            if not found_tokens and not literals.strip():
                st.info("This appears to be a simple literal match.")
    
    with regex_tab3:
        st.subheader("Pattern Generator from Examples")
        st.caption("Provide examples and we'll generate a matching pattern")
        source_text = st.text_input("Source data to search", value="£ 10, $3, $  50, £5",
                                   help="Text containing the values you want to match")
        examples_input = st.text_area("Example matches (one per line)", value="$3\n$50", height=100,
                                     help="Enter the specific values you want to match")
        space_mode = st.radio("Whitespace handling", ["Exact", "Flexible"], horizontal=True,
                            help="Exact: match spaces exactly. Flexible: allow optional spaces")
        if st.button("Generate Pattern", type="primary", key="gen_pattern"):
            if not source_text or not examples_input.strip():
                st.error("Please provide both source data and examples.")
            else:
                def example_to_pattern(example: str) -> str:
                    chunks = re.findall(r'\d+|[a-zA-Z]+|[^0-9a-zA-Z\s]+', example)
                    parts = []
                    for c in chunks:
                        if c.isdigit():
                            parts.append(r"\d+")
                        elif c.isalpha():
                            parts.append(r"[a-zA-Z]+")
                        else:
                            parts.append(re.escape(c))
                    joiner = r"\s*" if space_mode == "Flexible" else r"\s+"
                    return joiner.join(parts)
                examples = [e.strip() for e in examples_input.splitlines() if e.strip()]
                patterns = [example_to_pattern(e) for e in examples]
                final_pattern = "|".join(f"({p})" for p in patterns)
                st.markdown("**Generated Pattern:**")
                st.code(final_pattern, language="regex")
                try:
                    matches = re.findall(final_pattern, source_text)
                    if matches:
                        st.success(f"✓ Pattern matches {len(matches)} occurrence(s)")
                        st.markdown("**Found matches:**")
                        for m in matches:
                            match_text = m if isinstance(m, str) else next(g for g in m if g)
                            st.write(f"- `{match_text}`")
                    else:
                        st.warning("Pattern is valid, but no matches were found in the source data.")
                except re.error as e:
                    st.error(f"Generated pattern has an error: {e}")