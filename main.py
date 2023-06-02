import sys
import time
import asyncio
import logging
from src.BatchMessageProcessor import BatchMessageProcessor


async def main(logger: logging.Logger, batch_size, output_folder):
    logger.info("==== main ====")

    while True:
        bmp = None
        try:
            bmp = BatchMessageProcessor(batch_size=int(batch_size), output_folder=output_folder, logger=logger)
            await bmp.Run()
        except Exception:
            logger.exception("Exception while running")
        finally:
            if bmp is not None:
                bmp.Close()
        time.sleep(60)


if __name__ == "__main__":
    # args : batch_size, output_folder, log_level
    if len(sys.argv) == 4:
        # Todo: validate arguments.
        batch_size = sys.argv[1]
        output_folder = sys.argv[2]
        log_level = sys.argv[3]

        logging.basicConfig(level=log_level)
        logger = logging.getLogger()

        logger.info(f'BatchMessageProcessor started with; batch_size={batch_size}, output_folder={output_folder}')
        asyncio.run(main(logger, batch_size, output_folder))
    else:
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger()
        logger.error(f'3 argument required, only {len(sys.argv)-1} provided.')
