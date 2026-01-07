from scrapper.scrapy import main
from Algorithm.filter import filter_pipeline
from Algorithm.cluster import clustering_pipeline
import time, asyncio

import logging,sys
logger = logging.getLogger("runner")
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

async def pipeline():
    ''' Total Pipeline Process '''
    logging.info("Pipeline Process Initialized")
    start = time.time()
    #await main()
    filter_pipeline()
    clustering_pipeline()
    end = time.time()
    logging.info(f"Pipeline Process Completed. Time Taken {end-start:.2f} seconds")

if __name__ == "__main__":
        asyncio.run(pipeline())
