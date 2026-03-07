import unittest
from unittest.mock import MagicMock, patch
from mock_generator import MockDataGenerator
from db_interface import AS400DB2Interface

class TestMockGenerator(unittest.TestCase):
    def setUp(self):
        self.gen = MockDataGenerator()

    def test_increment(self):
        val1 = self.gen.generate_value("increment", "field1")
        val2 = self.gen.generate_value("increment", "field1")
        self.assertEqual(val1, 1)
        self.assertEqual(val2, 2)

    def test_faker_method(self):
        val = self.gen.generate_value("name")
        self.assertIsInstance(val, str)
        self.assertGreater(len(val), 0)

    def test_random_element(self):
        elements = ['A', 'B', 'C']
        val = self.gen.generate_value("random_element(['A', 'B', 'C'])")
        self.assertIn(val, elements)

    def test_random_int(self):
        val = self.gen.generate_value("random_int(min=10, max=20)")
        self.assertTrue(10 <= val <= 20)

class TestDBInterface(unittest.TestCase):
    def setUp(self):
        self.config = {
            "database": {
                "driver_path": "mock.jar",
                "driver_class": "mock.Driver",
                "connection_url": "jdbc:mock",
                "username": "user",
                "password": "pass",
                "schema": "MOCK"
            }
        }
        self.db = AS400DB2Interface(self.config)

    @patch('jaydebeapi.connect')
    def test_connect(self, mock_connect):
        self.db.connect()
        mock_connect.assert_called_once()

    def test_bulk_insert_success(self):
        self.db.conn = MagicMock()
        mock_cursor = self.db.conn.cursor.return_value
        
        cols = ['C1', 'C2']
        data = [[1, 'A'], [2, 'B']]
        
        s, f = self.db.execute_bulk_insert('TABLE', cols, data)
        self.assertEqual(s, 2)
        self.assertEqual(f, 0)
        mock_cursor.executemany.assert_called_once()
        self.db.conn.commit.assert_called_once()

    def test_bulk_insert_failure(self):
        self.db.conn = MagicMock()
        mock_cursor = self.db.conn.cursor.return_value
        mock_cursor.executemany.side_effect = Exception("DB Error")
        
        cols = ['C1', 'C2']
        data = [[1, 'A']]
        
        s, f = self.db.execute_bulk_insert('TABLE', cols, data)
        self.assertEqual(s, 0)
        self.assertEqual(f, 1)
        self.db.conn.rollback.assert_called_once()
    
    def test_get_table_columns(self):
        self.db.conn = MagicMock()
        mock_cursor = self.db.conn.cursor.return_value
        mock_cursor.fetchall.return_value = [
            ('C1', 'INTEGER', None, 10, 0),
            ('C2', 'VARCHAR', 50, None, None)
        ]
        cols = self.db.get_table_columns('TABLE')
        self.assertEqual(len(cols), 2)
        self.assertEqual(cols[0]['name'], 'C1')
        self.assertEqual(cols[1]['type'], 'VARCHAR')

    def test_get_journal_info(self):
        self.db.conn = MagicMock()
        mock_cursor = self.db.conn.cursor.return_value
        mock_cursor.fetchone.side_effect = [
            ('LIB', 'JRN'), # SYSTABLESTAT
            ('RLIB', 'RCV') # JOURNAL_INFO
        ]
        j_info = self.db.get_journal_info('TABLE')
        self.assertEqual(j_info['journal_name'], 'JRN')
        self.assertEqual(j_info['receiver_name'], 'RCV')

if __name__ == "__main__":
    unittest.main()
