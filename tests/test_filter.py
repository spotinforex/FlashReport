if __name__ == "__main__":
    #from scrapper.scrapy import main
    #import asyncio
    import time
    start = time.time()
    #asyncio.run(main())
    response = filter_pipeline()
    end = time.time()
    print(f" Time Taken: {end-start:.2f} seconds")
