import asyncio
import gzip
import logging
import json
import os
from stream_manager import (
    MessageStreamDefinition,
    ReadMessagesOptions,
    ResourceNotFoundException,
    StrategyOnFull,
    StreamManagerClient,
    StreamManagerException,
    ValidationException,
    NotEnoughMessagesException
)
from stream_manager.util import Util


class BatchJSONMessageProcessor:
    """BatchJSONMessageProcessor reads JSON messages from a stream and writes batches of them into a gzip file."""

    __stream_name = "BatchMessageStream"
    __output_folder = "./batched_messages/"
    __batch_id = 0

    def __init__(self, batch_size, output_folder, interval, logger: logging.Logger, client: StreamManagerClient = None):
        self.__client = client
        if self.__client is None:
            self.__client = StreamManagerClient()
        self.__logger = logger
        self.__interval = interval
        self.__batch_size = batch_size
        self.__output_folder = output_folder

        logger.debug(f"BatchJSONMessageProcessor initialized with batch_size={batch_size}, output_folder={output_folder}")

        # Try deleting the stream (if it exists) so that we have a fresh start
        try:
            self.__client.delete_message_stream(stream_name=self.__stream_name)
        except ResourceNotFoundException:
            pass

        # Create the message stream.
        self.__client.create_message_stream(
            MessageStreamDefinition(name=self.__stream_name,
                                    strategy_on_full=StrategyOnFull.OverwriteOldestData)
        )

    def __is_valid_json(self, message):
        """Check if a message is a valid JSON or not."""
        try:
            json.loads(message)
            return True
        except ValueError:
            return False

    async def __read_messages(self, under_test):
        """Read messages from the stream and batch them."""
        self.__logger.info("==== __read_messages  start ====")
        next_seq = 0
        keep_looping = True
        while keep_looping:
            try:
                self.__logger.info("Reading messages from stream")
                messages_list = self.__client.read_messages(
                    self.__stream_name,
                    ReadMessagesOptions(
                        desired_start_sequence_number=next_seq,
                        min_message_count=self.__batch_size,
                        max_message_count=self.__batch_size + 10,
                        read_timeout_millis=1000)
                )

                batched_messages = []
                for message in messages_list:
                    msg_decoded = message.payload.decode()
                    if self.__is_valid_json(msg_decoded):
                        batched_messages.append(msg_decoded)
                    else:
                        self.__logger.warning(f"Invalid JSON message: {msg_decoded}")
                self.__logger.info(f"Read {len(batched_messages)} messages from stream")
                self.__logger.debug(f"Messages: {batched_messages}")

                if len(batched_messages) >= self.__batch_size:
                    await self.__write_to_gzip(batched_messages)
                    
                # Update the next_seq here to ensure the next batch starts from the correct sequence
                if messages_list:
                    next_seq = messages_list[-1].sequence_number + 1

            except NotEnoughMessagesException:
                # Ignore this exception, it doesn't mean something went wrong.
                pass
            except Exception:
                self.__logger.exception("Exception while reading messages")
            await asyncio.sleep(self.__interval)
            keep_looping= not under_test

    async def __write_to_gzip(self, batched_messages):
        """Write batched messages to a gzip file."""
        self.__logger.info("==== __write_to_gzip  start ====")
        os.makedirs(self.__output_folder, exist_ok=True)
        file_path = os.path.join(self.__output_folder, f"batch_{self.__batch_id}.jsonl.gz")
        with gzip.open(file_path, 'wt') as f:
            for message in batched_messages:
                json_obj = json.loads(message)  # Parse the JSON object
                json_str = json.dumps(json_obj, separators=(',', ':'))  # Convert it back to string in compact representation
                f.write(json_str + '\n')
        self.__logger.info(f"Successfully wrote batch_{self.__batch_id} to {file_path}")
        self.__batch_id += 1

    async def Run(self, under_test=False):
        await self.__read_messages(under_test=under_test)

    def Close(self):
        self.__client.close()
