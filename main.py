import sys
import time
import asyncio
import logging
from src.BatchJSONMessageProcessor import BatchJSONMessageProcessor


async def main(logger: logging.Logger, batch_size, output_folder, interval):
    logger.info("==== main ====")

    while True:
        mjmp = None
        try:
            mjmp = BatchJSONMessageProcessor(batch_size=int(batch_size), output_folder=output_folder, interval=interval, logger=logger)
            await mjmp.Run()
        except Exception:
            logger.exception("Exception while running")
        finally:
            if mjmp is not None:
                mjmp.Close()
        logger.info("Something went wrong, wait a minute before trying again")
        time.sleep(60)


if __name__ == "__main__":
    # args : batch_size, output_folder, interval, log_level
    if len(sys.argv) == 5:
        batch_size = sys.argv[1]
        output_folder = sys.argv[2]
        interval = sys.argv[3]
        log_level = sys.argv[4]

        logging.basicConfig(level=log_level)
        logger = logging.getLogger()

        logger.info(f'BatchJSONMessageProcessor started with; batch_size={batch_size}, output_folder={output_folder}, interval={interval}')
        asyncio.run(main(logger, batch_size, output_folder, int(interval)))
    else:
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger()
        logger.error(f'4 argument required, only {len(sys.argv)-1} provided.')
