import asyncio
import glob
import os
import ntpath
import logging

from dataclasses import dataclass
from urllib.parse import urlparse

from stream_manager import (
    ExportDefinition,
    MessageStreamDefinition,
    ReadMessagesOptions,
    ResourceNotFoundException,
    S3ExportTaskDefinition,
    S3ExportTaskExecutorConfig,
    Status,
    StatusConfig,
    StatusLevel,
    StatusMessage,
    StrategyOnFull,
    StreamManagerClient,
    ValidationException,
    NotEnoughMessagesException,
)
from stream_manager.util import Util


@dataclass
class UploaderConfig:
    bucket_name: str
    prefix: str
    interval: int
    path: str


class DirectoryUploader:
    """DirectoryUploader monitors a folder for new files and upload those new files to S3 via stream manager."""

    def __init__(
        self,
        config: UploaderConfig,
        logger: logging.Logger,
        client: StreamManagerClient = None,
    ):
        """Initialize DirectoryUploader."""

        # Configuration parameters
        self.pathname = config.path
        self.bucket_name = config.bucket_name
        self.stream_name = config.bucket_name + "Stream"
        self.status_stream_name = self.stream_name + "Status"
        self.client = client
        self.prefix = (
            config.prefix if config.prefix.endswith("/") else config.prefix + "/"
        )
        self.logger = logger
        self.status_interval = min(config.interval, 1)
        self.interval = config.interval
        self.files_processed = set()

        if self.client is None:
            self.client = StreamManagerClient()

        logger.debug(f"DirectoryUploader initialized with {config}")

        # Delete existing streams (if any) for a fresh start.
        self._delete_existing_streams()

        # Create new streams for this session.
        self._create_streams()

    def _delete_existing_streams(self):
        """Delete existing streams if they exist."""

        # Delete the status stream for a fresh start.
        try:
            self.client.delete_message_stream(stream_name=self.status_stream_name)
        except ResourceNotFoundException:
            pass

        # Delete the main stream for a fresh start.
        try:
            self.client.delete_message_stream(stream_name=self.stream_name)
        except ResourceNotFoundException:
            pass

    def _create_streams(self):
        """Create new streams for the current session."""

        # Prepare an export definition.
        exports = ExportDefinition(
            s3_task_executor=[
                S3ExportTaskExecutorConfig(
                    identifier="S3TaskExecutor" + self.stream_name,  # Required
                    status_config=StatusConfig(
                        status_level=StatusLevel.INFO,
                        status_stream_name=self.status_stream_name,
                    ),
                )
            ]
        )

        # Create the Status Stream.
        self.client.create_message_stream(
            MessageStreamDefinition(
                name=self.status_stream_name,
                strategy_on_full=StrategyOnFull.OverwriteOldestData,
            )
        )

        # Create the message stream with the S3 Export definition.
        self.client.create_message_stream(
            MessageStreamDefinition(
                name=self.stream_name,
                strategy_on_full=StrategyOnFull.OverwriteOldestData,
                export_definition=exports,
            )
        )

    async def _scan(self, under_test=False):
        """Scan the directory for new files and upload them."""

        keep_looping = True
        while keep_looping:
            try:
                # Check if the directory exists and has appropriate permissions.
                base_dir = os.path.dirname(self.pathname)
                if ntpath.isdir(base_dir) and os.access(
                    base_dir, os.R_OK | os.W_OK | os.X_OK
                ):
                    self.logger.debug(
                        f"Scanning directory {self.pathname} for changes."
                    )

                    # Get all files sorted by modified time.
                    files = sorted(glob.glob(self.pathname), key=os.path.getmtime)
                    if files:
                        # Remove most recent file as it is considered the active file
                        self.logger.debug(f"The current active file is: {files.pop()}")

                    # Identify new files.
                    new_files = set(files) - self.files_processed

                    if not new_files:
                        self.logger.debug("No new files to transfer.")

                    # Process each new file.
                    for file in new_files:
                        await self._append_s3_task(file)

                    # Update the list of processed files.
                    self.files_processed = set(files)

                    await asyncio.sleep(self.interval)
                else:
                    self.logger.error(
                        f"The path {base_dir} is not a directory, does not exist or doesn't have sufficient (rwx) access."
                    )
                    if not under_test:
                        await asyncio.sleep(60)
            except Exception:
                self.logger.exception("Exception while scanning directory")
            keep_looping = not under_test

    async def _append_s3_task(self, file):
        """Append a S3 Task definition to the stream and log the sequence number."""

        # Prepare the S3 Task definition.
        head, tail = ntpath.split(file)
        key_with_partition = f"{self.prefix}year=!{{timestamp:YYYY}}/month=!{{timestamp:MM}}/day=!{{timestamp:dd}}/hour=!{{timestamp:HH}}/{tail}"
        s3_export_task_definition = S3ExportTaskDefinition(
            input_url=f"file://{file}",
            bucket=self.bucket_name,
            key=key_with_partition,
        )

        # Validate and serialize the S3 Task definition.
        try:
            payload = Util.validate_and_serialize_to_json_bytes(
                s3_export_task_definition
            )
        except ValidationException:
            self.logger.warning(
                f"Validation failed for file: {file}, bucket: {self.bucket_name}, key: {key_with_partition}. File not sent to S3."
            )
            return

        # Append the S3 Task definition to the stream.
        if payload is not None:
            sequence_number = self.client.append_message(self.stream_name, payload)
            self.logger.info(
                f"Successfully appended S3 Task Definition to stream with sequence number {sequence_number}."
            )

    async def _process_status(self, under_test=False):
        """Read the statuses from the export status stream."""

        next_seq = 0
        keep_looping = True
        while keep_looping:
            try:
                self.logger.debug("Reading messages from status stream.")

                # Read messages from the status stream.
                messages_list = self.client.read_messages(
                    self.status_stream_name,
                    ReadMessagesOptions(
                        desired_start_sequence_number=next_seq,
                        min_message_count=1,
                        max_message_count=5,
                        read_timeout_millis=1000,
                    ),
                )

                # Process each message.
                for message in messages_list:
                    if message.sequence_number is not None:
                        next_seq = message.sequence_number + 1
                    status_message = Util.deserialize_json_bytes_to_obj(
                        message.payload, StatusMessage
                    )
                    self._handle_status_message(status_message)

            except NotEnoughMessagesException:
                # Ignore this exception, as it doesn't indicate an error.
                pass
            except Exception:
                self.logger.exception("Exception while processing status")
            self.logger.debug(f"Sleeping for {self.status_interval} seconds")
            await asyncio.sleep(self.status_interval)
            keep_looping = not under_test

    def _handle_status_message(self, status_message):
        """Handle a status message."""

        file_url = status_message.status_context.s3_export_task_definition.input_url
        bucket = status_message.status_context.s3_export_task_definition.bucket
        key = status_message.status_context.s3_export_task_definition.key

        # Check the status of the status message.
        if status_message.status == Status.Success:
            self.logger.info(f"Successfully uploaded file at path {file_url} to s3://{bucket}/{key}")
            final_path = os.path.abspath(
                os.path.join(urlparse(file_url).netloc, urlparse(file_url).path)
            )
            os.remove(final_path)
        elif status_message.status == Status.InProgress:
            self.logger.info("File upload is in progress.")
        elif status_message.status in [Status.Failure, Status.Canceled]:
            self.logger.error(
                f"Unable to upload file at path {file_url} to S3. Message: {status_message.message}"
            )
            self.files_processed.remove(file_url.partition("file://")[2])

    async def run(self):
        """Run the DirectoryUploader."""

        tasks = [
            asyncio.create_task(self._scan()),
            asyncio.create_task(self._process_status()),
        ]
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

    def close(self):
        """Close the DirectoryUploader."""

        self.client.close()
