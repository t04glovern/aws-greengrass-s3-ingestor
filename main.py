import sys
import asyncio
import logging

from src.BatchJSONMessageProcessor import BatchJSONMessageProcessor
from src.DirectoryUploader import DirectoryUploader


async def process_messages(logger: logging.Logger, processor_stream_name, processor_batch_size, processor_path, processor_interval):
    while True:
        processor = None
        try:
            processor = BatchJSONMessageProcessor(
                stream_name=processor_stream_name, batch_size=int(processor_batch_size), output_folder=processor_path, interval=processor_interval, logger=logger)
            await processor.Run()
        except Exception:
            logger.exception("Exception while running")
        finally:
            if processor is not None:
                processor.Close()
        logger.info(
            "Something went wrong with message processing, wait a minute before trying again")
        await asyncio.sleep(60)


async def upload_directory(logger: logging.Logger, uploader_path, uploader_bucket_name, uploader_prefix, uploader_interval):
    while True:
        uploader = None
        try:
            uploader = DirectoryUploader(
                pathname=uploader_path, 
                bucket_name=uploader_bucket_name,
                prefix=uploader_prefix,
                interval=uploader_interval, 
                logger=logger)
            await uploader.Run()
        except Exception:
            logger.exception("Exception while running")
        finally:
            if uploader is not None:
                uploader.Close()
        logger.info(
            "Something went wrong with directory uploading, wait a minute before trying again")
        await asyncio.sleep(60)


async def main(logger: logging.Logger, processor_stream_name, processor_batch_size, processor_interval, processor_path, uploader_bucket_name, uploader_prefix, uploader_interval, uploader_path):
    processor_task = asyncio.create_task(process_messages(logger, processor_stream_name, processor_batch_size, processor_path, processor_interval))
    uploader_task = asyncio.create_task(upload_directory(logger, uploader_path, uploader_bucket_name, uploader_prefix, uploader_interval))

    await asyncio.gather(processor_task, uploader_task)


if __name__ == "__main__":
    if len(sys.argv) == 10:
        processor_stream_name = sys.argv[1]
        processor_batch_size = sys.argv[2]
        processor_interval = sys.argv[3]
        processor_path = sys.argv[4]
        uploader_bucket_name = sys.argv[5]
        uploader_prefix = sys.argv[6]
        uploader_interval = sys.argv[7]
        uploader_path = sys.argv[8]
        log_level = sys.argv[9]

        logging.basicConfig(level=log_level)
        logger = logging.getLogger()

        logger.info(
            f'Started with; processor_stream_name={processor_stream_name}, processor_batch_size={processor_batch_size}, processor_interval={processor_interval}, processor_path={processor_path}, uploader_bucket_name={uploader_bucket_name}, uploader_prefix={uploader_prefix}, uploader_interval={uploader_interval}, uploader_path={uploader_path}')
        asyncio.run(main(logger, processor_stream_name, processor_batch_size, int(processor_interval), processor_path, uploader_bucket_name, uploader_prefix, int(uploader_interval), uploader_path))
    else:
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger()
        logger.error(f'9 arguments required, only {len(sys.argv)-1} provided.')
