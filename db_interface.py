import abc
import jaydebeapi
import logging

class DatabaseInterface(abc.ABC):
    @abc.abstractmethod
    def connect(self):
        pass

    @abc.abstractmethod
    def close(self):
        pass

    @abc.abstractmethod
    def get_table_columns(self, table_name, schema=None):
        pass

    @abc.abstractmethod
    def schema_exists(self, schema):
        pass

    @abc.abstractmethod
    def get_journal_info(self, table_name, schema=None):
        pass

    @abc.abstractmethod
    def get_journal_entries_info(self, table_name, schema=None):
        pass

    @abc.abstractmethod
    def execute_bulk_insert(self, table_name, columns, data):
        pass

    @abc.abstractmethod
    def execute_bulk_update(self, table_name, columns, data, pk_column):
        pass

    @abc.abstractmethod
    def execute_bulk_delete(self, table_name, data, pk_column):
        pass

    @abc.abstractmethod
    def get_random_pks(self, table_name, pk_column, count):
        pass

class AS400DB2Interface(DatabaseInterface):
    def __init__(self, config):
        self.config = config
        self.conn = None
        self.logger = logging.getLogger(__name__)

    def connect(self):
        db_cfg = self.config['database']
        self.conn = jaydebeapi.connect(
            db_cfg['driver_class'],
            db_cfg['connection_url'],
            [db_cfg['username'], db_cfg['password']],
            db_cfg['driver_path']
        )
        self.logger.info("Connected to AS400 DB2")

    def close(self):
        if self.conn:
            self.conn.close()
            self.logger.info("Connection closed")

    def schema_exists(self, schema):
        cursor = self.conn.cursor()
        query = "SELECT 1 FROM QSYS2.SYSSCHEMAS WHERE SCHEMA_NAME = ?"
        cursor.execute(query, [schema.upper()])
        row = cursor.fetchone()
        cursor.close()
        return row is not None

    def get_table_columns(self, table_name, schema=None):
        cursor = self.conn.cursor()
        if schema is None:
            schema = self.config['database'].get('schema', '')
        # sanitize input by ensuring it's uppercase and only contains valid SQL identifiers
        table_name = table_name.upper()
        
        query = """
            SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE 
            FROM QSYS2.SYSCOLUMNS 
            WHERE TABLE_NAME = ?
        """
        params = [table_name]
        if schema:
            query += " AND TABLE_SCHEMA = ?"
            params.append(schema.upper())
        
        cursor.execute(query, params)
        columns = cursor.fetchall()
        cursor.close()
        # Return as list of dicts for easier comparison
        return [
            {
                "name": row[0].strip(),
                "type": row[1].strip(),
                "length": row[2],
                "precision": row[3],
                "scale": row[4]
            } for row in columns
        ]

    def get_journal_info(self, table_name, schema=None):
        cursor = self.conn.cursor()
        if schema is None:
            schema = self.config['database'].get('schema', '')
        table_name = table_name.upper()
        
        # Get journal name and library using JOURNALED_OBJECTS (more reliable across OS versions)
        query = "SELECT JOURNAL_LIBRARY, JOURNAL_NAME FROM QSYS2.JOURNALED_OBJECTS WHERE OBJECT_NAME = ? AND OBJECT_TYPE = '*FILE'"
        params = [table_name]
        if schema:
            query += " AND OBJECT_LIBRARY = ?"
            params.append(schema.upper())
        
        cursor.execute(query, params)
        row = cursor.fetchone()
        
        if not row:
            cursor.close()
            return None
        
        j_lib, j_name = row[0].strip(), row[1].strip()
        
        # Get current receiver name
        query_rcv = "SELECT JOURNAL_RECEIVER_LIBRARY, JOURNAL_RECEIVER_NAME FROM QSYS2.JOURNAL_INFO WHERE JOURNAL_LIBRARY = ? AND JOURNAL_NAME = ?"
        cursor.execute(query_rcv, [j_lib, j_name])
        rcv_row = cursor.fetchone()
        rcv_name = rcv_row[1].strip() if rcv_row else "UNKNOWN"
        
        cursor.close()
        return {
            "journal_library": j_lib,
            "journal_name": j_name,
            "receiver_name": rcv_name
        }

    def get_journal_entries_info(self, table_name, schema=None):
        cursor = self.conn.cursor()
        if schema is None:
            schema = self.config['database'].get('schema', '')
        table_name = table_name.upper()
        
        j_info = self.get_journal_info(table_name, schema=schema)
        if not j_info:
            return None
        
        # Use QSYS2.DISPLAY_JOURNAL to get entries for this specific table
        # This function can be heavy, we fetch oldest and newest entry sequence numbers
        query = f"""
            SELECT MIN(JOURNAL_SEQUENCE_NUMBER), MAX(JOURNAL_SEQUENCE_NUMBER)
            FROM TABLE(QSYS2.DISPLAY_JOURNAL('{j_info['journal_library']}', '{j_info['journal_name']}')) AS X
            WHERE OBJECT = ?
        """
        params = [table_name]
        if schema:
            query += " AND OBJECT_LIBRARY = ?"
            params.append(schema.upper())

        try:
            cursor.execute(query, params)
            row = cursor.fetchone()
            cursor.close()
            if row:
                return {
                    "oldest_sequence": row[0],
                    "newest_sequence": row[1],
                    "journal": f"{j_info['journal_library']}/{j_info['journal_name']}"
                }
        except Exception as e:
            self.logger.warning(f"Could not retrieve journal entries: {e}")
            cursor.close()
            
        return {
            "oldest_sequence": "UNKNOWN",
            "newest_sequence": "UNKNOWN",
            "info": "Requires proper authorities to QSYS2.DISPLAY_JOURNAL"
        }

    def execute_bulk_insert(self, table_name, columns, data):
        cursor = self.conn.cursor()
        placeholders = ",".join(["?" for _ in columns])
        sql = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders})"
        try:
            cursor.executemany(sql, data)
            self.conn.commit()
            return len(data), 0
        except Exception as e:
            self.logger.error(f"Bulk insert failed: {e}")
            self.conn.rollback()
            return 0, len(data)
        finally:
            cursor.close()

    def execute_bulk_update(self, table_name, columns, data, pk_column):
        cursor = self.conn.cursor()
        set_clause = ",".join([f"{col} = ?" for col in columns if col != pk_column])
        sql = f"UPDATE {table_name} SET {set_clause} WHERE {pk_column} = ?"
        
        processed_data = []
        pk_idx = columns.index(pk_column)
        for row in data:
            new_row = [val for i, val in enumerate(row) if i != pk_idx]
            new_row.append(row[pk_idx])
            processed_data.append(new_row)

        try:
            cursor.executemany(sql, processed_data)
            self.conn.commit()
            return len(data), 0
        except Exception as e:
            self.logger.error(f"Bulk update failed: {e}")
            self.conn.rollback()
            return 0, len(data)
        finally:
            cursor.close()

    def execute_bulk_delete(self, table_name, data, pk_column):
        cursor = self.conn.cursor()
        sql = f"DELETE FROM {table_name} WHERE {pk_column} = ?"
        try:
            cursor.executemany(sql, [[d] for d in data])
            self.conn.commit()
            return len(data), 0
        except Exception as e:
            self.logger.error(f"Bulk delete failed: {e}")
            self.conn.rollback()
            return 0, len(data)
        finally:
            cursor.close()

    def get_random_pks(self, table_name, pk_column, count):
        cursor = self.conn.cursor()
        query = f"SELECT {pk_column} FROM {table_name} FETCH FIRST {count} ROWS ONLY"
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        return [row[0] for row in rows]
