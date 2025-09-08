import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta, timezone

import bigtable_cleanup
import bigtable_cleanup as script_name  # 👈 your actual script file name

class TestBigtableCleanup(unittest.TestCase):

    def setUp(self):
        #Setup test data
        self.encoded_date = b'\x00\x06\x32\x03\xd0\x86\x0a\x40'
        self.hardcoded_microseconds = 1743841825000000
          # same timestamp in bytes
        self.row_key = bytes.fromhex("e6385effadd4283e9c2c8767185f14f0a2c0ccd0ab9c899921b5066f1ca788d5")
        self.ist_date = datetime(2025, 4, 5, 14, 00, 25)
        self.utc_date = self.ist_date - timedelta(hours=5, minutes=30)

        # self.encoded_date = b'\x00\x061\xa7\x9b\xc5\xe0\xd2'
        # self.hardcoded_microseconds = 1743445803000018
        # # same timestamp in bytes
        # self.row_key = bytes.fromhex("e6385effadd4283e9c2c8767185f14f0a2c0ccd0ab9c899921b5066f1ca788d5")
        # self.ist_date = datetime(2025, 4, 1, 00, 00, 3)
        # self.utc_date = self.ist_date - timedelta(hours=5, minutes=30)

    def test_decode_date_with_hardcoded_value(self):
        result = script_name.decode_date(self.encoded_date)
        self.assertEqual(result, self.hardcoded_microseconds)

    def test_convert_date_time_with_hardcoded_microseconds(self):
        result = script_name.convert_date_time(self.hardcoded_microseconds)
        self.assertEqual(result, self.ist_date)

    def test_get_day_difference_from_today(self):

        utc_now = datetime.now(timezone.utc)


        ist_date = utc_now + timedelta(hours=5, minutes=30)

        result = script_name.get_day_difference_from_today(self.ist_date)
        self.assertEqual(result, 11)




    def test_precondition_check(self):
        # Test that precondition_check returns True for days >= 15
        result = script_name.precondition_check(self.encoded_date)
        self.assertEqual(result,False)


    @patch('bigtable_cleanup.get_table_object')
    def test_extract_latest_update_at_from_key_with_mock(self, mock_get_table_object):
        mock_row = MagicMock()
        mock_cell = MagicMock(value=self.encoded_date)

        mock_row.cells = {
            "cf": {
                b"updated_at": [mock_cell]
            }
        }

        mock_table = MagicMock()
        mock_table.read_row.return_value = mock_row
        mock_get_table_object.return_value = mock_table

        result = script_name.extract_latest_update_at_from_key(self.row_key)
        self.assertEqual(result, self.encoded_date)

    @patch('bigtable_cleanup.get_table_object')
    def test_delete_row(self, mock_get_table_object):
        mock_row = MagicMock()
        mock_table = MagicMock()
        mock_table.row.return_value = mock_row
        mock_get_table_object.return_value = mock_table

        script_name.delete_row(self.row_key)
        mock_row.delete.assert_called_once()
        mock_row.commit.assert_called_once()

    @patch('bigtable_cleanup.extract_latest_update_at_from_key')
    @patch('bigtable_cleanup.delete_row')
    @patch('bigtable_cleanup.get_table_object')
    def test_process_rows(self, mock_get_table_object, mock_delete_row, mock_extract_latest):
        mock_row = MagicMock()
        mock_row.row_key = self.row_key

        mock_extract_latest.return_value = self.encoded_date
        mock_table = MagicMock()
        mock_table.read_rows.return_value = [mock_row]
        mock_get_table_object.return_value = mock_table

        script_name.process_rows()
        #mock_delete_row.assert_called_once_with(self.row_key)
        mock_delete_row.assert_not_called()


if __name__ == '__main__':
    unittest.main()
