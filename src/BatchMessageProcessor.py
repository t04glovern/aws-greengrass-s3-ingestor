import asyncio
import gzip
import logging
import json
import os

from datetime import datetime
from dataclasses import dataclass

from typing import List

from stream_manager import (
    MessageStreamDefinition,
    ReadMessagesOptions,
    ResourceNotFoundException,
    StrategyOnFull,
    StreamManagerClient,
    NotEnoughMessagesException,
)
from stream_manager.data import Message


@dataclass
class ProcessorConfig:
    """Data class representing the configuration for the BatchMessageProcessor.

    Attributes:
        stream_name (str): Name of the message stream to be processed.
        batch_size (int): The size of each batch of messages to be written to a file.
        interval (int): Time interval (in seconds) to wait between reading messages.
        path (str): Path to the directory where the gzip files will be saved.
    """
    stream_name: str
    batch_size: int
    interval: int
    path: str


class BatchMessageProcessor:
    """Reads JSON messages from a stream and writes batches of them into a gzip file.

    Attributes:
        client (StreamManagerClient): Client to manage the message stream. If not provided, 
            a default StreamManagerClient instance will be created.
        logger (logging.Logger): Logger instance for logging messages and exceptions.
        interval (int): Time interval (in seconds) to wait between reading messages.
        batch_size (int): The size of each batch of messages to be written to a file.
        output_folder (str): Path to the directory where the gzip files will be saved.
        stream_name (str): Name of the message stream to be processed.
        batch_id (int): Counter for the batches processed.
    """

    def __init__(
        self,
        config: ProcessorConfig,
        logger: logging.Logger,
        client: StreamManagerClient = None,
    ):
        """Initializes BatchMessageProcessor with the given configuration, logger, and client.

        Args:
            config (ProcessorConfig): Configuration for the processor.
            logger (logging.Logger): Logger instance for logging messages and exceptions.
            client (StreamManagerClient, optional): Client to manage the message stream. 
                If not provided, a default StreamManagerClient instance will be created.
        """

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
        """Prepares the message stream for use by the BatchMessageProcessor.

        This method checks if the message stream exists and creates it if it doesn't.
        """

        stream_definition = MessageStreamDefinition(
            name=self.stream_name,
            strategy_on_full=StrategyOnFull.OverwriteOldestData,
        )

        try:
            self.client.describe_message_stream(stream_name=self.stream_name)
            self.logger.info(f"Stream {self.stream_name} already exists. Updating it...")
            self.client.update_message_stream(stream_definition=stream_definition)
        except StreamManagerException:
            self.logger.info(f"Creating stream {self.stream_name}...")
            self.client.create_message_stream(stream_definition=stream_definition)

    @staticmethod
    def _is_valid_json(message: str) -> bool:
        """Checks if a given string is a valid JSON.

        Args:
            message (str): Message string to be validated.

        Returns:
            bool: True if the message is a valid JSON, False otherwise.
        """

        try:
            json.loads(message)
            return True
        except ValueError:
            return False

    async def _read_messages(self, under_test: bool=False):
        """Reads messages from the stream and writes them into gzip files in batches.

        This method reads messages from the stream, validates them, and writes 
        them to a gzip file if the batch size is reached.

        Args:
            under_test (bool, optional): Flag to determine if the function is 
                being executed under a test environment. Defaults to False.
        """

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
                valid_messages = []
                for message in messages_list:
                    decoded_payload = message.payload.decode()
                    if self._is_valid_json(decoded_payload):
                        valid_messages.append(decoded_payload)

                self.logger.info(f"Read {len(valid_messages)} messages from stream")
                self.logger.debug(f"Messages: {valid_messages}")

                # Write valid messages into a gzip file if the batch size is reached.
                if len(valid_messages) >= self.batch_size:
                    await self._write_to_gzip(valid_messages)

                # Update the sequence number for the next batch.
                if messages_list:
                    next_seq = messages_list[-1].sequence_number + 1

            except StreamManagerException as e:
                self.logger.error(f"StreamManagerException occurred: {e}")
                # Maybe add some retries or specific handling based on the exception details.
            except ConnectionError:
                self.logger.error("Connection error occurred. Retrying in a few seconds...")
                await asyncio.sleep(5)  # Wait for 5 seconds before retrying.
            except asyncio.TimeoutError:
                self.logger.warning("Request to StreamManager timed out. Retrying...")
                await asyncio.sleep(1)  # Wait for 1 second before retrying.
            except Exception as e:
                self.logger.exception(f"Unexpected error: {e}")
                await asyncio.sleep(5)  # Wait for 5 seconds before retrying in case of any other unexpected error.

            await asyncio.sleep(self.interval)
            keep_looping = not under_test

    async def _write_to_gzip(self, valid_messages: List[str]) -> None:
        """Writes valid messages into a gzip file.

        Args:
            valid_messages (List[str]): List of valid JSON messages to be written to the gzip file.
        """

        os.makedirs(self.output_folder, exist_ok=True)
        date_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        file_path = os.path.join(self.output_folder, f"{date_str}_{self.batch_id}.jsonl.gz")

        with gzip.open(file_path, "wt") as f:
            for message in valid_messages:
                json_obj = json.loads(message)
                json_str = json.dumps(json_obj, separators=(",", ":"))
                f.write(json_str + "\n")

        self.logger.info(f"Successfully wrote batch {self.batch_id} to {file_path}")
        self.batch_id += 1

    async def run(self, under_test: bool=False):
        """Starts the message reading process.

        Args:
            under_test (bool, optional): Flag to determine if the function is 
                being executed under a test environment. Defaults to False.
        """

        await self._read_messages(under_test=under_test)

    def close(self):
        """Closes the client connection to the message stream."""
        
        self.client.close()
