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
    def execute_bulk_insert(self, table_name, columns, data, schema=None):
        pass

    @abc.abstractmethod
    def execute_bulk_update(self, table_name, columns, data, pk_column, schema=None):
        pass

    @abc.abstractmethod
    def execute_bulk_delete(self, table_name, data, pk_column, schema=None):
        pass

    @abc.abstractmethod
    def get_random_pks(self, table_name, pk_column, count, schema=None):
        pass

    @abc.abstractmethod
    def get_max_id(self, table_name, pk_column, schema=None):
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
        query_rcv = "SELECT ATTACHED_JOURNAL_RECEIVER_LIBRARY, ATTACHED_JOURNAL_RECEIVER_NAME FROM QSYS2.JOURNAL_INFO WHERE JOURNAL_LIBRARY = ? AND JOURNAL_NAME = ?"
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
            SELECT MIN(SEQUENCE_NUMBER), MAX(SEQUENCE_NUMBER)
            FROM TABLE(QSYS2.DISPLAY_JOURNAL('{j_info['journal_library']}', '{j_info['journal_name']}')) AS X
            WHERE OBJECT = ?
        """
        params = [table_name]
        # Some OS versions might not have OBJECT_LIBRARY column in DISPLAY_JOURNAL.
        # If it fails, we will try without the OBJECT_LIBRARY filter.

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

    def _qualify_table(self, table_name, schema):
        if schema:
            return f"{schema.upper()}.{table_name.upper()}"
        return table_name.upper()

    def execute_bulk_insert(self, table_name, columns, data, schema=None):
        cursor = self.conn.cursor()
        full_table_name = self._qualify_table(table_name, schema)
        placeholders = ",".join(["?" for _ in columns])
        sql = f"INSERT INTO {full_table_name} ({','.join(columns)}) VALUES ({placeholders})"
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

    def execute_bulk_update(self, table_name, columns, data, pk_column, schema=None):
        cursor = self.conn.cursor()
        full_table_name = self._qualify_table(table_name, schema)
        set_clause = ",".join([f"{col} = ?" for col in columns if col != pk_column])
        sql = f"UPDATE {full_table_name} SET {set_clause} WHERE {pk_column} = ?"
        
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

    def execute_bulk_delete(self, table_name, data, pk_column, schema=None):
        cursor = self.conn.cursor()
        full_table_name = self._qualify_table(table_name, schema)
        sql = f"DELETE FROM {full_table_name} WHERE {pk_column} = ?"
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

    def get_random_pks(self, table_name, pk_column, count, schema=None):
        cursor = self.conn.cursor()
        full_table_name = self._qualify_table(table_name, schema)
        query = f"SELECT {pk_column} FROM {full_table_name} FETCH FIRST {count} ROWS ONLY"
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        return [row[0] for row in rows]

    def get_max_id(self, table_name, pk_column, schema=None):
        cursor = self.conn.cursor()
        full_table_name = self._qualify_table(table_name, schema)
        query = f"SELECT MAX({pk_column}) FROM {full_table_name}"
        cursor.execute(query)
        row = cursor.fetchone()
        cursor.close()
        return row[0] if row and row[0] is not None else 0
