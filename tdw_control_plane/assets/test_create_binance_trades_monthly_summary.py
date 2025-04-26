import unittest
from unittest.mock import patch, MagicMock
import os

from create_binance_trades_monthly_summary import create_binance_trades_monthly_summary


class TestCreateBinanceTradesMonthylSummary(unittest.TestCase):
    
    @patch('create_binance_trades_monthly_summary.ClickhouseClient')
    def test_table_creation_success(self, mock_client):
        """Test successful creation of the monthly summary table."""
        # Mock context
        context = MagicMock()
        context.log = MagicMock()
        
        # Mock the client execution responses in sequence
        mock_client_instance = MagicMock()
        # For database existence check
        # For table existence check
        # For row count check after backfill
        # For min date check
        # For max date check
        mock_client_instance.execute.side_effect = [
            [(1,)],  # Database exists
            [(0,)],  # Table doesn't exist
            [(10,)],  # 10 rows inserted
            [('2019-01-01',)],  # Min date
            [('2023-01-01',)],  # Max date
        ]
        mock_client.return_value = mock_client_instance
        
        # Execute the asset
        result = create_binance_trades_monthly_summary(context)
        
        # Verify results
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['action'], 'created')
        self.assertEqual(result['row_count'], 10)
        self.assertEqual(result['date_range'], '2019-01-01 to 2023-01-01')
        
        # Verify the correct SQL statements were executed
        # Should have 5 execute calls:
        # 1. Database existence check
        # 2. Table existence check
        # 3. Create table statement
        # 4. Insert data statement
        # 5. Count rows
        # 6. Get min date
        # 7. Get max date
        self.assertEqual(mock_client_instance.execute.call_count, 7)
        
        # Check table creation SQL contains the correct structure
        create_call = mock_client_instance.execute.call_args_list[2]
        create_sql = create_call[0][0]
        self.assertIn('CREATE TABLE', create_sql)
        self.assertIn('month_start', create_sql)
        self.assertIn('total_trades', create_sql)
        self.assertIn('ENGINE = MergeTree', create_sql)
        
        # Check insert SQL has the correct GROUP BY structure
        insert_call = mock_client_instance.execute.call_args_list[3]
        insert_sql = insert_call[0][0]
        self.assertIn('INSERT INTO', insert_sql)
        self.assertIn('SELECT', insert_sql)
        self.assertIn('GROUP BY month_start', insert_sql)
        self.assertIn('ORDER BY month_start', insert_sql)
    
    @patch('create_binance_trades_monthly_summary.ClickhouseClient')
    def test_table_recreation(self, mock_client):
        """Test recreation of the table when it already exists."""
        # Mock context
        context = MagicMock()
        context.log = MagicMock()
        
        # Mock the client execution responses
        mock_client_instance = MagicMock()
        mock_client_instance.execute.side_effect = [
            [(1,)],  # Database exists
            [(1,)],  # Table exists
            None,    # Drop table result
            None,    # Create table result
            None,    # Insert data result
            [(5,)],  # 5 rows inserted
            [('2020-01-01',)],  # Min date
            [('2022-01-01',)],  # Max date
        ]
        mock_client.return_value = mock_client_instance
        
        # Execute the asset
        result = create_binance_trades_monthly_summary(context)
        
        # Verify results
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['action'], 'recreated')
        self.assertEqual(result['row_count'], 5)
        
        # Verify the correct SQL statements were executed
        # Should include the DROP TABLE statement
        drop_call = mock_client_instance.execute.call_args_list[2]
        drop_sql = drop_call[0][0]
        self.assertIn('DROP TABLE IF EXISTS', drop_sql)
    
    @patch('create_binance_trades_monthly_summary.ClickhouseClient')
    def test_database_not_exists(self, mock_client):
        """Test handling when the database doesn't exist."""
        # Mock context
        context = MagicMock()
        context.log = MagicMock()
        
        # Mock the client execution responses
        mock_client_instance = MagicMock()
        mock_client_instance.execute.return_value = [(0,)]  # Database doesn't exist
        mock_client.return_value = mock_client_instance
        
        # Execute the asset
        result = create_binance_trades_monthly_summary(context)
        
        # Verify results
        self.assertEqual(result['status'], 'error')
        self.assertIn('Database', result['message'])
        
    @patch('create_binance_trades_monthly_summary.ClickhouseClient')
    def test_exception_handling(self, mock_client):
        """Test exception handling during execution."""
        # Mock context
        context = MagicMock()
        context.log = MagicMock()
        
        # Mock the client to raise an exception
        mock_client_instance = MagicMock()
        mock_client_instance.execute.side_effect = Exception('Test exception')
        mock_client.return_value = mock_client_instance
        
        # Execute the asset
        result = create_binance_trades_monthly_summary(context)
        
        # Verify results
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['message'], 'Test exception')
        
        # Verify disconnect was called even with an exception
        mock_client_instance.disconnect.assert_called_once()


if __name__ == '__main__':
    unittest.main() 