import json
from unittest.mock import MagicMock, patch
import main
import sys

def test_main_flow():
    # Mocking DB Interface
    with patch('main.AS400DB2Interface') as MockDB:
        mock_db = MockDB.return_value
        # Mock connection
        mock_db.connect.return_value = None
        
        # Mock get_table_columns for CUSTOMERZ
        def mock_get_columns(table):
            if table == 'CUSTOMERZ':
                return [
                    {'name': 'CUST_ID', 'type': 'INTEGER', 'length': None, 'precision': 10, 'scale': 0},
                    {'name': 'FIRST_NAME', 'type': 'VARCHAR', 'length': 50, 'precision': None, 'scale': None},
                    {'name': 'LAST_NAME', 'type': 'VARCHAR', 'length': 50, 'precision': None, 'scale': None},
                    {'name': 'EMAIL', 'type': 'VARCHAR', 'length': 100, 'precision': None, 'scale': None},
                    {'name': 'POSTAL_CODE', 'type': 'VARCHAR', 'length': 10, 'precision': None, 'scale': None},
                    {'name': 'CREATED_AT', 'type': 'TIMESTAMP', 'length': None, 'precision': None, 'scale': None}
                ]
            elif table == 'PRODUCTS':
                return [
                    {'name': 'PROD_ID', 'type': 'INTEGER', 'length': None, 'precision': 10, 'scale': 0},
                    {'name': 'PROD_NAME', 'type': 'VARCHAR', 'length': 100, 'precision': None, 'scale': None},
                    {'name': 'CATEGORY', 'type': 'VARCHAR', 'length': 50, 'precision': None, 'scale': None},
                    {'name': 'PRICE', 'type': 'DECIMAL', 'length': None, 'precision': 10, 'scale': 2},
                    {'name': 'STOCK_QTY', 'type': 'INTEGER', 'length': None, 'precision': 10, 'scale': 0}
                ]
            elif table == 'ORDERS':
                return [
                    {'name': 'ORDER_ID', 'type': 'INTEGER', 'length': None, 'precision': 10, 'scale': 0},
                    {'name': 'CUST_ID', 'type': 'INTEGER', 'length': None, 'precision': 10, 'scale': 0},
                    {'name': 'ORDER_DATE', 'type': 'DATE', 'length': None, 'precision': None, 'scale': None},
                    {'name': 'TOTAL_AMOUNT', 'type': 'DECIMAL', 'length': None, 'precision': 12, 'scale': 2},
                    {'name': 'STATUS', 'type': 'VARCHAR', 'length': 20, 'precision': None, 'scale': None}
                ]
            return []

        mock_db.get_table_columns.side_effect = mock_get_columns
        
        # Mock journal info
        mock_db.get_journal_info.return_value = {
            "journal_library": "LIB", "journal_name": "JRN", "receiver_name": "RCV"
        }
        mock_db.get_journal_entries_info.return_value = {
            "oldest_sequence": 100, "newest_sequence": 200
        }
        
        # Mock inserts/updates/deletes
        mock_db.execute_bulk_insert.return_value = (60, 0)
        mock_db.get_random_pks.return_value = [1, 2, 3]
        mock_db.execute_bulk_update.return_value = (30, 0)
        mock_db.execute_bulk_delete.return_value = (10, 0)
        
        # Run main
        try:
            main.main()
        except SystemExit as e:
            print(f"Main exited with code {e.code}")

if __name__ == "__main__":
    test_main_flow()
