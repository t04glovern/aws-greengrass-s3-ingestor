import argparse
import asyncio
import logging

from src.BatchMessageProcessor import BatchMessageProcessor, ProcessorConfig
from src.DirectoryUploader import DirectoryUploader, UploaderConfig


async def process_messages(logger: logging.Logger, config: ProcessorConfig):
    while True:
        processor = None
        try:
            processor = BatchMessageProcessor(config, logger)
            await processor.run()
        except Exception:
            logger.exception("Exception while running")
        finally:
            if processor is not None:
                processor.close()
        logger.info(
            "Something went wrong with message processing, wait a minute before trying again"
        )
        await asyncio.sleep(60)


async def upload_directory(logger: logging.Logger, config: UploaderConfig):
    while True:
        uploader = None
        try:
            uploader = DirectoryUploader(config, logger)
            await uploader.run()
        except Exception:
            logger.exception("Exception while running")
        finally:
            if uploader is not None:
                uploader.close()
        logger.info(
            "Something went wrong with directory uploading, wait a minute before trying again"
        )
        await asyncio.sleep(60)


async def main(
    logger: logging.Logger,
    processor_config: ProcessorConfig,
    uploader_config: UploaderConfig,
):
    processor_task = asyncio.create_task(process_messages(logger, processor_config))
    uploader_task = asyncio.create_task(upload_directory(logger, uploader_config))

    await asyncio.gather(processor_task, uploader_task)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--path")
    parser.add_argument("--pattern", default="*")
    parser.add_argument("--interval", type=int)
    parser.add_argument("--processor_stream_name")
    parser.add_argument("--processor_batch_size", type=int)
    parser.add_argument("--uploader_bucket_name")
    parser.add_argument("--uploader_prefix")
    parser.add_argument("--log_level")

    args = parser.parse_args()

    processor_config = ProcessorConfig(
        stream_name=args.processor_stream_name,
        batch_size=args.processor_batch_size,
        interval=args.interval,
        path=args.path,
    )

    uploader_config = UploaderConfig(
        bucket_name=args.uploader_bucket_name,
        prefix=args.uploader_prefix,
        interval=args.interval,
        path="{}/{}".format(args.path, args.pattern),
    )

    logging.basicConfig(level=args.log_level)
    logger = logging.getLogger()

    logger.info(
        f"Started with; processor_config={processor_config}, uploader_config={uploader_config}"
    )
    asyncio.run(main(logger, processor_config, uploader_config))
