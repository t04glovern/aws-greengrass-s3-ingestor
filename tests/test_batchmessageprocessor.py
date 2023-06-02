import unittest
import unittest.mock
import tempfile
import logging
import asyncio
import os
import gzip

from src.BatchMessageProcessor import BatchMessageProcessor
from stream_manager.data import Message

class TestBatchMessageProcessor(unittest.TestCase):
    def test_process_messages(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            mock_client = unittest.mock.MagicMock()
            read_messages_mock = unittest.mock.MagicMock()
            mock_client.read_messages = read_messages_mock
            mock_messages = [
                Message(stream_name='stream1', sequence_number=i, ingest_time=1000, payload=bytes(m, 'utf-8')) 
                for i, m in enumerate(['message1', 'message2', 'message3'], start=1)
            ]
            read_messages_mock.return_value = mock_messages

            bmp = BatchMessageProcessor(batch_size=3, output_folder=tmpdirname, logger=logger, client=mock_client)
            loop = asyncio.get_event_loop()

            # Testing with some messages - output file should be created
            loop.run_until_complete(bmp.Run(under_test=True))
            self.assertTrue(os.path.exists(os.path.join(tmpdirname, "batch_0.gz")))

            # Testing with zero messages - no new file should be created
            read_messages_mock.return_value = []
            loop.run_until_complete(bmp.Run(under_test=True))
            self.assertFalse(os.path.exists(os.path.join(tmpdirname, "batch_1.gz")))

            # Verify that the output file contains the expected messages
            with gzip.open(os.path.join(tmpdirname, "batch_0.gz"), 'rt') as f:
                lines = f.readlines()
                self.assertEqual(lines[0], 'message1\n')
                self.assertEqual(lines[1], 'message2\n')
                self.assertEqual(lines[2], 'message3\n')


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

if __name__ == '__main__':
    unittest.main()