import unittest
import unittest.mock
import tempfile
import logging
import asyncio
import os
import gzip
from datetime import datetime

from src.BatchMessageProcessor import BatchMessageProcessor, ProcessorConfig

from stream_manager import (
    NotEnoughMessagesException,
    ResourceNotFoundException,
    StreamManagerException
)
from stream_manager.data import Message

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class TestBatchMessageProcessor(unittest.TestCase):
    @unittest.mock.patch("src.BatchMessageProcessor.datetime")
    def test_process_messages(self, mock_datetime: datetime):
        # Mock datetime to return a specific value
        mock_datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0) # type: ignore
        mock_datetime.datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0) # for direct calls to datetime.datetime # type: ignore

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

    @unittest.mock.patch("src.BatchMessageProcessor.datetime")
    @unittest.mock.patch("asyncio.sleep", return_value=None)  # Mocking sleep to avoid real delays
    def test_handle_StreamManagerException(self, _, mock_datetime: datetime):
        mock_datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0) # type: ignore
        mock_datetime.datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0) # for direct calls to datetime.datetime # type: ignore

        with tempfile.TemporaryDirectory() as tmpdirname:
            mock_client = unittest.mock.MagicMock()
            mock_client.read_messages.side_effect = StreamManagerException("Mock Exception")

            config = ProcessorConfig(stream_name="stream1", batch_size=3, path=tmpdirname, interval=1)
            bmp = BatchMessageProcessor(config, logger, client=mock_client)
            loop = asyncio.get_event_loop()

            with self.assertLogs(logger, level="ERROR") as cm:
                loop.run_until_complete(bmp.run(under_test=True))
                self.assertIn("ERROR:root:StreamManagerException occurred: Mock Exception", cm.output)

    @unittest.mock.patch("src.BatchMessageProcessor.datetime")
    @unittest.mock.patch("asyncio.sleep", return_value=None)
    def test_handle_ConnectionError(self, _, mock_datetime: datetime):
        mock_datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0) # type: ignore
        mock_datetime.datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0) # for direct calls to datetime.datetime # type: ignore

        with tempfile.TemporaryDirectory() as tmpdirname:
            mock_client = unittest.mock.MagicMock()
            mock_client.read_messages.side_effect = ConnectionError("Mock Connection Error")

            config = ProcessorConfig(stream_name="stream1", batch_size=3, path=tmpdirname, interval=1)
            bmp = BatchMessageProcessor(config, logger, client=mock_client)
            loop = asyncio.get_event_loop()

            with self.assertLogs(logger, level="ERROR") as cm:
                loop.run_until_complete(bmp.run(under_test=True))
                self.assertIn("ERROR:root:Connection error occurred. Retrying in a few seconds...", cm.output)

    @unittest.mock.patch("src.BatchMessageProcessor.datetime")
    @unittest.mock.patch("asyncio.sleep", return_value=None)
    def test_handle_TimeoutError(self, _, mock_datetime: datetime):
        mock_datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0) # type: ignore
        mock_datetime.datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0) # for direct calls to datetime.datetime # type: ignore

        with tempfile.TemporaryDirectory() as tmpdirname:
            mock_client = unittest.mock.MagicMock()
            mock_client.read_messages.side_effect = asyncio.TimeoutError("Mock Timeout Error")

            config = ProcessorConfig(stream_name="stream1", batch_size=3, path=tmpdirname, interval=1)
            bmp = BatchMessageProcessor(config, logger, client=mock_client)
            loop = asyncio.get_event_loop()

            with self.assertLogs(logger, level="WARNING") as cm:
                loop.run_until_complete(bmp.run(under_test=True))
                self.assertIn("WARNING:root:Request to StreamManager timed out. Retrying...", cm.output)

    @unittest.mock.patch("src.BatchMessageProcessor.datetime")
    def test_handle_StreamManagerException_on_prepare_stream(self, mock_datetime: datetime):
        mock_datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0) # type: ignore
        mock_datetime.datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0) # for direct calls to datetime.datetime # type: ignore

        with tempfile.TemporaryDirectory() as tmpdirname:
            mock_client = unittest.mock.MagicMock()

            config = ProcessorConfig(stream_name="stream1", batch_size=3, path=tmpdirname, interval=1)

            # Verify the logger message
            with self.assertLogs(logger, level="INFO") as cm:
                _ = BatchMessageProcessor(config, logger, client=mock_client)
                self.assertIn(f"INFO:root:Creating stream {config.stream_name}...", cm.output)

            # Verify that create_message_stream was called after the exception
            mock_client.create_message_stream.assert_called_once_with(definition=unittest.mock.ANY)

    @unittest.mock.patch("src.BatchMessageProcessor.datetime")
    def test_handle_NotEnoughMessagesException(self, mock_datetime: datetime):
        mock_datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0) # type: ignore
        mock_datetime.datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0) # for direct calls to datetime.datetime # type: ignore

        with tempfile.TemporaryDirectory() as tmpdirname:
            mock_client = unittest.mock.MagicMock()
            # Mock read_messages to raise NotEnoughMessagesException
            mock_client.read_messages.side_effect = NotEnoughMessagesException("Mock Not Enough Messages")

            config = ProcessorConfig(stream_name="stream1", batch_size=3, path=tmpdirname, interval=1)
            bmp = BatchMessageProcessor(config, logger, client=mock_client)
            loop = asyncio.get_event_loop()

            # Running the processor to check if it handles the NotEnoughMessagesException without crashing
            loop.run_until_complete(bmp.run(under_test=True))

    @unittest.mock.patch("src.BatchMessageProcessor.datetime")
    @unittest.mock.patch("asyncio.sleep", return_value=None)  # Mocking sleep to avoid real delays
    def test_handle_generic_exception(self, _, mock_datetime: datetime):
        mock_datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0) # type: ignore
        mock_datetime.datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0) # for direct calls to datetime.datetime # type: ignore

        with tempfile.TemporaryDirectory() as tmpdirname:
            mock_client = unittest.mock.MagicMock()
            # Mock read_messages to raise a generic exception
            mock_client.read_messages.side_effect = RuntimeError("Mock RuntimeError")

            config = ProcessorConfig(stream_name="stream1", batch_size=3, path=tmpdirname, interval=1)
            bmp = BatchMessageProcessor(config, logger, client=mock_client)
            loop = asyncio.get_event_loop()

            with self.assertLogs(logger, level="ERROR") as cm:
                loop.run_until_complete(bmp.run(under_test=True))
                logged_messages = [msg for msg in cm.output if "Unexpected error: Mock RuntimeError" in msg]
                self.assertTrue(logged_messages)  # Ensure that the list is not empty

            # Check that asyncio.sleep was called with both 5 and 1 seconds delay
            _.assert_has_calls([unittest.mock.call(5), unittest.mock.call(1)])

    def test_close_method(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            mock_client = unittest.mock.MagicMock()

            config = ProcessorConfig(stream_name="stream1", batch_size=3, path=tmpdirname, interval=1)
            bmp = BatchMessageProcessor(config, logger, client=mock_client)

            # Call the close method
            bmp.close()

            # Verify that the close method of the client was called
            mock_client.close.assert_called_once()

if __name__ == "__main__":
    unittest.main()
