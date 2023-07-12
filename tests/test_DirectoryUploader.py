import unittest
import unittest.mock
import tempfile
import logging
import asyncio
import os

from src.DirectoryUploader import DirectoryUploader, UploaderConfig
from stream_manager import (
    StatusMessage,
    S3ExportTaskDefinition,
    EventType,
    StatusLevel,
    Status,
    StatusContext,
)
from stream_manager.data import Message
from stream_manager.util import Util


class TestDirectoryUploader(unittest.TestCase):
    def test_scan(self):
        tmpdir = tempfile.mkdtemp()
        mock_client = unittest.mock.MagicMock()
        append_mock = unittest.mock.MagicMock()
        append_mock.return_value = 123
        mock_client.append_message = append_mock

        config = UploaderConfig(
            bucket_name="test-bucket",
            prefix="sample",
            interval=1,
            path=tmpdir + "/*.csv",
        )
        du = DirectoryUploader(config, logger, client=mock_client)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(du._DirectoryUploader__scan(under_test=True))
        mock_client.assert_not_called()
        f = open(tmpdir + "/test1.csv", "a")
        f.write("test file 1!")
        f.close()
        loop.run_until_complete(du._DirectoryUploader__scan(under_test=True))
        mock_client.assert_not_called()
        f = open(tmpdir + "/test2.csv", "a")
        f.write("test file 2!")
        f.close()
        loop.run_until_complete(du._DirectoryUploader__scan(under_test=True))
        append_mock.assert_called()

    def test_ProcessStatus(self):
        tmpdir = tempfile.mkdtemp()
        filename = tmpdir + "/test1.csv"
        f = open(filename, "a")
        f.write("test file 1!")
        f.close()

        task_def = S3ExportTaskDefinition(
            input_url="file://" + filename, bucket="bucket", key="key"
        )
        status_context = StatusContext(
            s3_export_task_definition=task_def, sequence_number=123
        )
        status_message = StatusMessage(
            event_type=EventType.S3Task,
            status_level=StatusLevel.INFO,
            status=Status.InProgress,
            status_context=status_context,
            message="message",
            timestamp_epoch_ms=1,
        )
        payload = Util.validate_and_serialize_to_json_bytes(status_message)
        test_message = Message(payload=payload)
        message_list = [test_message]

        mock_client = unittest.mock.MagicMock()
        read_messages_mock = unittest.mock.MagicMock()
        read_messages_mock.return_value = message_list
        mock_client.read_messages = read_messages_mock

        config = UploaderConfig(
            bucket_name="test-bucket", prefix="", interval=1, path=tmpdir + "/*.csv"
        )
        du = DirectoryUploader(config, logger, client=mock_client)
        loop = asyncio.get_event_loop()

        loop.run_until_complete(du._DirectoryUploader__processStatus(under_test=True))
        self.assertTrue(os.path.exists(filename))

        status_message.status = Status.Success
        payload = Util.validate_and_serialize_to_json_bytes(status_message)
        test_message = Message(payload=payload)
        message_list = [test_message]
        read_messages_mock.return_value = message_list

        loop.run_until_complete(du._DirectoryUploader__processStatus(under_test=True))
        self.assertFalse(os.path.exists(filename))

    def test_scan_dirnotexist(self):
        fakedir = "/does/not/exists/*.cvs"
        mock_client = unittest.mock.MagicMock()
        append_mock = unittest.mock.MagicMock()
        append_mock.return_value = 123
        mock_client.append_message = append_mock

        mock_logger = unittest.mock.MagicMock()
        mock_error = unittest.mock.MagicMock()
        mock_logger.error = mock_error

        config = UploaderConfig(
            bucket_name="test-bucket", prefix="sample", interval=1, path=fakedir
        )
        du = DirectoryUploader(config, logger=mock_logger, client=mock_client)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(du._DirectoryUploader__scan(under_test=True))
        mock_client.assert_not_called()
        mock_error.assert_called_once()

    def test_wrongpath(self):
        # testing what happens if the wildchar is not in the file name.
        # this should get caught as an invalid directory
        tmpdir = tempfile.mkdtemp()
        testdir = tmpdir + "/testdir"
        os.mkdir(testdir)
        f = open(testdir + "/test1.csv", "a")
        f.write("test file 1!")
        f.close()

        mock_client = unittest.mock.MagicMock()
        append_mock = unittest.mock.MagicMock()
        append_mock.return_value = 123
        mock_client.append_message = append_mock

        mock_logger = unittest.mock.MagicMock()
        mock_error = unittest.mock.MagicMock()
        mock_logger.error = mock_error

        config = UploaderConfig(
            bucket_name="test-bucket",
            prefix="sample",
            interval=1,
            path=tmpdir + "/test*/test1.csv",
        )
        du = DirectoryUploader(config, logger=mock_logger, client=mock_client)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(du._DirectoryUploader__scan(under_test=True))
        mock_client.assert_not_called()
        mock_error.assert_called_once()


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

if __name__ == "__main__":
    unittest.main()
