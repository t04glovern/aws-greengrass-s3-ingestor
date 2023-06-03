import sys
import time
import asyncio
import logging
from src.BatchJSONMessageProcessor import BatchJSONMessageProcessor


async def main(logger: logging.Logger, stream_name, batch_size, output_folder, interval):
    logger.info("==== main ====")

    while True:
        processor = None
        try:
            processor = BatchJSONMessageProcessor(stream_name=stream_name, batch_size=int(
                batch_size), output_folder=output_folder, interval=interval, logger=logger)
            await processor.Run()
        except Exception:
            logger.exception("Exception while running")
        finally:
            if processor is not None:
                processor.Close()
        logger.info("Something went wrong, wait a minute before trying again")
        time.sleep(60)


if __name__ == "__main__":
    # args : stream_name, batch_size, output_folder, interval, log_level
    if len(sys.argv) == 6:
        stream_name = sys.argv[1]
        batch_size = sys.argv[2]
        output_folder = sys.argv[3]
        interval = sys.argv[4]
        log_level = sys.argv[5]

        logging.basicConfig(level=log_level)
        logger = logging.getLogger()

        logger.info(
            f'BatchJSONMessageProcessor started with; stream_name={stream_name}, batch_size={batch_size}, output_folder={output_folder}, interval={interval}')
        asyncio.run(main(logger, stream_name, batch_size, output_folder, int(interval)))
    else:
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger()
        logger.error(f'5 argument required, only {len(sys.argv)-1} provided.')
