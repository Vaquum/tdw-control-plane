import unittest
from unittest.mock import patch, MagicMock
import hashlib
import os
from io import BytesIO
from datetime import datetime

from monthly_trades_to_tdw import _process_month


class TestMonthlyTradesToTdw(unittest.TestCase):
    
    @patch('monthly_trades_to_tdw.requests')
    @patch('monthly_trades_to_tdw.zipfile')
    @patch('monthly_trades_to_tdw.ClickhouseClient')
    def test_clickhouse_client_timeout(self, mock_client, mock_zipfile, mock_requests):
        """Test that ClickHouse client is created with the correct timeout parameter."""
        # Mock context
        context = MagicMock()
        context.log = MagicMock()
        
        # Mock requests responses
        mock_checksum_response = MagicMock()
        mock_checksum_response.text = 'abcdef1234567890 *BTCUSDT-trades-2020-01.zip'
        mock_checksum_response.raise_for_status = MagicMock()
        
        mock_data_response = MagicMock()
        mock_data_response.content = b'fake_zip_data'
        mock_data_response.raise_for_status = MagicMock()
        
        mock_requests.get.side_effect = [mock_checksum_response, mock_data_response]
        
        # Mock zipfile
        mock_zip_ref = MagicMock()
        mock_zip_ref.__enter__.return_value = mock_zip_ref
        mock_zip_ref.namelist.return_value = ['BTCUSDT-trades-2020-01.csv']
        
        mock_csv_file = MagicMock()
        mock_csv_file.__enter__.return_value = mock_csv_file
        mock_csv_file.read.return_value = b'trade_id,price,qty,quote_qty,time,is_buyer_maker,is_best_match\n1,8000.0,1.0,8000.0,1577836800000,true,true'
        
        mock_zip_ref.open.return_value = mock_csv_file
        mock_zipfile.ZipFile.return_value = mock_zip_ref
        
        # Mock hashlib
        real_sha256 = hashlib.sha256
        
        def mock_sha256(data=None):
            m = real_sha256()
            if data:
                m.update(data)
            return m
        
        hashlib.sha256 = mock_sha256
        
        # Mock the client execution
        mock_client_instance = MagicMock()
        mock_client_instance.execute.return_value = [(0,)]  # Return for count check
        mock_client.return_value = mock_client_instance
        
        # Run with exception to prevent full execution
        try:
            _process_month(context, 'BTCUSDT-trades-2020-01.zip')
        except Exception:
            # We expect an exception since we're not fully mocking everything
            pass
        
        # Check that client was initialized with correct timeout
        mock_client.assert_called_once()
        args, kwargs = mock_client.call_args
        self.assertEqual(kwargs.get('connect_timeout'), 300)
        
        # Reset hashlib
        hashlib.sha256 = real_sha256


if __name__ == '__main__':
    unittest.main() 