import unittest
import unittest.mock
import tempfile
import logging
import asyncio
import os
import gzip
from datetime import datetime

from src.BatchMessageProcessor import BatchMessageProcessor, ProcessorConfig
from stream_manager.data import Message


class TestBatchMessageProcessor(unittest.TestCase):
    @unittest.mock.patch("src.BatchMessageProcessor.datetime")
    def test_process_messages(self, mock_datetime):
        # Mock datetime to return a specific value
        mock_datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0)
        mock_datetime.datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0)  # for direct calls to datetime.datetime

        with tempfile.TemporaryDirectory() as tmpdirname:
            mock_client = unittest.mock.MagicMock()
            read_messages_mock = unittest.mock.MagicMock()
            mock_client.read_messages = read_messages_mock

            # Mock messages should be valid JSON strings now.
            mock_messages = [
                Message(
                    stream_name="stream1",
                    sequence_number=i,
                    ingest_time=1000,
                    payload=bytes('{"message": "' + m + '"}', "utf-8"),
                )
                for i, m in enumerate(["message1", "message2", "message3"], start=1)
            ]
            invalid_json_message = Message(
                stream_name="stream1",
                sequence_number=1,
                ingest_time=1000,
                payload=bytes("test", "utf-8"),
            )
            mock_messages.append(invalid_json_message)

            read_messages_mock.return_value = mock_messages

            config = ProcessorConfig(
                stream_name="stream1", batch_size=3, path=tmpdirname, interval=1
            )
            bmp = BatchMessageProcessor(config, logger, client=mock_client)
            loop = asyncio.get_event_loop()

            expected_file = os.path.join(tmpdirname, "2023-01-01_12-00-00_0.jsonl.gz")

            # Testing with some messages - output file should be created
            loop.run_until_complete(bmp.run(under_test=True))
            self.assertTrue(os.path.exists(expected_file))

            # Testing with zero messages - no new file should be created
            read_messages_mock.return_value = []
            loop.run_until_complete(bmp.run(under_test=True))
            self.assertFalse(
                os.path.exists(os.path.join(tmpdirname, "batch_1.jsonl.gz"))
            )

            # Verify that the output file contains the expected messages
            with gzip.open(expected_file, "rt") as f:
                lines = f.readlines()
                self.assertEqual(lines[0], '{"message":"message1"}\n')
                self.assertEqual(lines[1], '{"message":"message2"}\n')
                self.assertEqual(lines[2], '{"message":"message3"}\n')


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

if __name__ == "__main__":
    unittest.main()
