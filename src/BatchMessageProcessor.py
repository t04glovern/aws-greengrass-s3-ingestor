import asyncio
import gzip
import logging
import json
import os

from dataclasses import dataclass

from stream_manager import (
    MessageStreamDefinition,
    ReadMessagesOptions,
    ResourceNotFoundException,
    StrategyOnFull,
    StreamManagerClient,
    NotEnoughMessagesException,
)


@dataclass
class ProcessorConfig:
    stream_name: str
    batch_size: int
    interval: int
    path: str


class BatchMessageProcessor:
    """Reads JSON messages from a stream and writes batches of them into a gzip file."""

    def __init__(
        self,
        config: ProcessorConfig,
        logger: logging.Logger,
        client: StreamManagerClient = None,
    ):
        """Initializes BatchMessageProcessor with the given configuration, logger, and client."""

        self.client = client or StreamManagerClient()
        self.logger = logger
        self.interval = config.interval
        self.batch_size = config.batch_size
        self.output_folder = config.path
        self.stream_name = config.stream_name
        self.batch_id = 0

        self.logger.debug(f"BatchMessageProcessor initialized with {config}")

        self._prepare_stream()

    def _prepare_stream(self):
        """Prepares the message stream for use by the BatchMessageProcessor."""

        # Delete existing stream for a fresh start.
        try:
            self.client.delete_message_stream(stream_name=self.stream_name)
        except ResourceNotFoundException:
            pass

        # Create a new message stream.
        self.client.create_message_stream(
            MessageStreamDefinition(
                name=self.stream_name,
                strategy_on_full=StrategyOnFull.OverwriteOldestData,
            )
        )

    @staticmethod
    def _is_valid_json(message: str) -> bool:
        """Checks if a given string is a valid JSON."""

        try:
            json.loads(message)
            return True
        except ValueError:
            return False

    async def _read_messages(self, under_test=False):
        """Reads messages from the stream and writes them into gzip files in batches."""

        next_seq = 0
        keep_looping = True
        while keep_looping:
            try:
                self.logger.debug("Reading messages from stream")

                # Read messages from the stream.
                messages_list = self.client.read_messages(
                    self.stream_name,
                    ReadMessagesOptions(
                        desired_start_sequence_number=next_seq,
                        min_message_count=self.batch_size,
                        max_message_count=self.batch_size * 10,
                        read_timeout_millis=1000,
                    ),
                )

                # Extract and validate messages.
                valid_messages = [
                    message.payload.decode()
                    for message in messages_list
                    if self._is_valid_json(message.payload.decode())
                ]

                self.logger.info(f"Read {len(valid_messages)} messages from stream")
                self.logger.debug(f"Messages: {valid_messages}")

                # Write valid messages into a gzip file if the batch size is reached.
                if len(valid_messages) >= self.batch_size:
                    await self._write_to_gzip(valid_messages)

                # Update the sequence number for the next batch.
                if messages_list:
                    next_seq = messages_list[-1].sequence_number + 1

            except NotEnoughMessagesException:
                pass
            except Exception:
                self.logger.exception("Exception while reading messages")
            await asyncio.sleep(self.interval)
            keep_looping = not under_test

    async def _write_to_gzip(self, valid_messages):
        """Writes valid messages into a gzip file."""

        os.makedirs(self.output_folder, exist_ok=True)
        file_path = os.path.join(self.output_folder, f"batch_{self.batch_id}.jsonl.gz")

        with gzip.open(file_path, "wt") as f:
            for message in valid_messages:
                json_obj = json.loads(message)
                json_str = json.dumps(json_obj, separators=(",", ":"))
                f.write(json_str + "\n")

        self.logger.info(f"Successfully wrote batch_{self.batch_id} to {file_path}")
        self.batch_id += 1

    async def run(self, under_test=False):
        """Starts the message reading process."""

        await self._read_messages(under_test=under_test)

    def close(self):
        """Closes the client connection."""

        self.client.close()