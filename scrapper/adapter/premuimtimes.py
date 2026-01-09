import logging
import sys
logger = logging.getLogger("runner")
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

from bs4 import BeautifulSoup
from datetime import datetime
import time

def parse_premuimtimes_news(html_content):
    """
    Parse Premium Times website content and extract articles
    Args:
        html_content: Raw Html Data 
    Returns:
        parsed_data: Parsed Data in Json Format 
    """
    try:
        start = time.time()
        soup = BeautifulSoup(html_content, 'lxml')
        
        parsed_data = {
            'source': 'Premium Times Nigeria',
            'source_url': 'https://www.premiumtimesng.com',
            'scraped_at': datetime.now().isoformat(),
            'articles': []
        }
        
        # Parse Hero/Featured Article
        logging.info("Parsing Premium Times Hero Article Section In Progress")
        hero_section = soup.find('article', class_='jeg_post jeg_hero_item_1')
        if hero_section:
            article = {}
            
            title_elem = hero_section.find('h2', class_='jeg_post_title')
            if title_elem and title_elem.find('a'):
                article['title'] = title_elem.find('a').get_text(strip=True)

                article['news_url'] = title_elem.find('a').get('href', '')
            img_elem = hero_section.find('div', class_='thumbnail-container')
            if img_elem and img_elem.find('img'):
                article['image_url'] = img_elem.find('img').get('data-src') or img_elem.find('img').get('src', '')

            if not img_elem:
                    article['image_url'] = None
            
            
            meta_date = hero_section.find('div', class_='jeg_meta_date')
            if meta_date and meta_date.find('a'):
                article['published_at'] = meta_date.find('a').get_text(strip=True)
            
            if article.get('title'):
                parsed_data['articles'].append(article)
        logging.info("Parsing Premium Times Hero Article Section Completed")
        
        # Parse Top News/Headlines Section
        logging.info("Parsing Premium Times Top News Section In Progress")
        top_news_blocks = soup.find_all('div', class_='jeg_postblock_21')
        for block in top_news_blocks:
            header = block.find('div', class_='jeg_block_heading')
            section_name = 'Top News'
            if header and header.find('h3'):
                section_name = header.find('h3').get_text(strip=True)
            
            articles_list = block.find_all('article', class_='jeg_post')
            for item in articles_list:
                article = {}
                
                title_elem = item.find('h3', class_='jeg_post_title')
                if title_elem and title_elem.find('a'):
                    article['title'] = title_elem.find('a').get_text(strip=True)
                    article['news_url'] = title_elem.find('a').get('href', '')
                
                img_elem = item.find('div', class_='thumbnail-container')
                if img_elem and img_elem.find('img'):
                    article['image_url'] = img_elem.find('img').get('data-src') or img_elem.find('img').get('src', '')

                if not img_elem:
                    article['image_url'] = None
                meta_date = item.find('div', class_='jeg_meta_date')
                if meta_date and meta_date.find('a'):
                    article['published_at'] = meta_date.find('a').get_text(strip=True)
                
                if article.get('title'):
                    parsed_data['articles'].append(article)
        logging.info("Parsing Premium Times Top News Section Completed")
        
        # Parse More News Section
        logging.info("Parsing Premium Times More News Section In Progress")
        more_news_blocks = soup.find_all('div', class_='jeg_postblock_22')
        for block in more_news_blocks:
            header = block.find('div', class_='jeg_block_heading')
            section_name = 'More News'
            if header and header.find('h3'):
                section_name = header.find('h3').get_text(strip=True)
            
            articles_list = block.find_all('article', class_='jeg_post')
            for item in articles_list:
                article = {}
                
                title_elem = item.find('h3', class_='jeg_post_title')
                if title_elem and title_elem.find('a'):
                    article['title'] = title_elem.find('a').get_text(strip=True)
                    article['news_url'] = title_elem.find('a').get('href', '')
                img_elem = item.find('div', class_='thumbnail-container')
                if img_elem and img_elem.find('img'):
                    article['image_url'] = img_elem.find('img').get('data-src') or img_elem.find('img').get('src', '')
                if not img_elem:
                    article['image_url'] = None
                meta_date = item.find('div', class_='jeg_meta_date')
                if meta_date and meta_date.find('a'):
                    article['published_at'] = meta_date.find('a').get_text(strip=True)
                if article.get('title'):
                    parsed_data['articles'].append(article)
        logging.info("Parsing Premium Times More News Section Completed")
        
        # Parse Special Reports/Investigations Section
        logging.info("Parsing Premium Times Special Reports Section In Progress")
        investigation_blocks = soup.find_all('div', class_='jeg_postblock_39')
        for block in investigation_blocks:
            header = block.find('div', class_='jeg_block_heading')
            section_name = 'Special Reports'
            if header and header.find('h3'):
                section_name = header.find('h3').get_text(strip=True)
            
            articles_list = block.find_all('article', class_='jeg_post')
            for item in articles_list:
                article = {}
                
                title_elem = item.find('h3', class_='jeg_post_title')
                if title_elem and title_elem.find('a'):
                    article['title'] = title_elem.find('a').get_text(strip=True)
                    article['news_url'] = title_elem.find('a').get('href', '') 
                img_elem = item.find('div', class_='thumbnail-container')
                if img_elem and img_elem.find('img'):
                    article['image_url'] = img_elem.find('img').get('data-src') or img_elem.find('img').get('src', '')
                if not img_elem:
                    article['image_url'] = None
                
                meta_date = item.find('div', class_='jeg_meta_date')
                if meta_date and meta_date.find('a'):
                    article['published_at'] = meta_date.find('a').get_text(strip=True)
                
                if article.get('title'):
                    parsed_data['articles'].append(article)
        logging.info("Parsing Premium Times Special Reports Section Completed")
        
        # Parse Newsfeed Carousel
        logging.info("Parsing Premium Times Newsfeed Carousel In Progress")
        carousel = soup.find('div', class_='jeg_newsfeed_list')
        if carousel:
            newsfeed_items = carousel.find_all('div', class_='jeg_newsfeed_item')
            for item in newsfeed_items:
                article = {}
                
                title_elem = item.find('h3', class_='jeg_post_title')
                if title_elem and title_elem.find('a'):
                    article['title'] = title_elem.find('a').get_text(strip=True)
                    article['news_url'] = title_elem.find('a').get('href', '')
                
                img_elem = item.find('img')
                if img_elem:
                    article['image_url'] = img_elem.get('data-src') or img_elem.get('src', '')
                if not img_elem:
                    article['image_url'] = None
                if article.get('title'):
                    parsed_data['articles'].append(article)
        logging.info("Parsing Premium Times Newsfeed Carousel Completed")
        
        # Parse Promoted Content
        logging.info("Parsing Premium Times Promoted Content Section In Progress")
        promoted_section = soup.find('div', {'class': 'jeg_postblock_21', 'data-unique': lambda x: x and 'promoted' in str(x).lower()})
        if not promoted_section:
            # Alternative search
            all_blocks = soup.find_all('div', class_='jeg_block_heading')
            for block in all_blocks:
                if block.find('h3') and 'promoted' in block.find('h3').get_text(strip=True).lower():
                    promoted_section = block.parent
                    break
        
        if promoted_section:
            articles_list = promoted_section.find_all('article', class_='jeg_post')
            for item in articles_list:
                article = {}
                
                title_elem = item.find('h3', class_='jeg_post_title')
                if title_elem and title_elem.find('a'):
                    article['title'] = title_elem.find('a').get_text(strip=True)
                    article['news_url'] = title_elem.find('a').get('href', '')
                
                img_elem = item.find('div', class_='thumbnail-container')
                if img_elem and img_elem.find('img'):
                    article['image_url'] = img_elem.find('img').get('data-src') or img_elem.find('img').get('src', '')
                if not img_elem:
                    article['image_url'] = None
                meta_date = item.find('div', class_='jeg_meta_date')
                if meta_date and meta_date.find('a'):
                    article['published_at'] = meta_date.find('a').get_text(strip=True)
                
                if article.get('title'):
                    parsed_data['articles'].append(article)
        logging.info("Parsing Premium Times Promoted Content Section Completed")
        
        # Remove duplicates based on title
        logging.info("Removing Duplicates In Progress")
        seen_titles = set()
        unique_articles = []
        for article in parsed_data['articles']:
            if article['title'] not in seen_titles:
                seen_titles.add(article['title'])
                unique_articles.append(article)
        
        parsed_data['articles'] = unique_articles
        
        end = time.time()
        
        logging.info(f"Parsing Completed. Total Articles: {len(unique_articles)}. Time Taken: {end - start:.2f} seconds.")
        
        return parsed_data
        
    except Exception as e:
        logging.error(f"An Error Occurred When Parsing Premium Times Data: {e}")
        return None

