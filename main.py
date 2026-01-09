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
    scrap = await main()
    fill = filter_pipeline()
    clus = clustering_pipeline()
    end = time.time()
    logging.info(f"Pipeline Process Completed. Time Taken {end-start:.2f} seconds")
    if scrap and fill and clus:
        return "Pipeline Ran Successfully"
    else:
        return "Failed To Complete Pipeline Run"

if __name__ == "__main__":
    import asyncio
    asyncio.run(pipeline())
    

