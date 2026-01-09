import logging,sys
logger = logging.getLogger("runner")
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

from bs4 import BeautifulSoup
from datetime import datetime
from pathlib import Path
import time

def parse_punch_news(html_content):
    """
    Parse Punch News website content and extract articles
    Args:
        html_content: Raw Html Data 
    Returns:
        parsed_data: Parsed Data in Json Format 
    """
    try:
        start = time.time()
        soup = BeautifulSoup(html_content, 'lxml')
        
        parsed_data = {
            'source': 'Punch Nigeria',
            'source_url': 'https://punchng.com',
            'scraped_at': datetime.now().isoformat(),
            'articles': []
        }
        
        # Parse Latest News section
        logging.info("Parsing Punch Newspaper Latest News Section In Progress")
        latest_news_section = soup.find('div', class_='just-in-timeline')
        if latest_news_section:
            for item in latest_news_section.find_all('li', class_='new-item'):
                article = {}
                
                # Extract title
                title_elem = item.find('h3', class_='entry-title')
                if title_elem and title_elem.find('a'):
                    article['title'] = title_elem.find('a').get_text(strip=True)
                    article['news_url'] = title_elem.find('a').get('href', '')
                    
                    article['image_url'] = None
                
                # Extract time
                time_elem = item.find('div', class_='meta-time')
                if time_elem:
                    article['published_at'] = time_elem.get_text(strip=True)
                
                if article.get('title'):
                    parsed_data['articles'].append(article)
        logging.info("Parsing Punch Newspaper Latest News Section Completed")
        
        # Parse Featured Article
        logging.info("Parsing Punch Newspaper Featured Article Section In Progress")
        featured = soup.find('div', class_='feature-article')
        if featured:
            article = {}
            
            title_elem = featured.find('h2', class_='post-title')
            if title_elem and title_elem.find('a'):
                article['title'] = title_elem.find('a').get_text(strip=True)
                article['news_url'] = title_elem.find('a').get('href', '')
                
            
            img_elem = featured.find('img')
            if img_elem:
                article['image_url'] = img_elem.get('data-src') or img_elem.get('src', '')
            if not img_elem:
                article['image_url'] = None
            if article.get('title'):
                parsed_data['articles'].append(article)
        logging.info("Parsing Punch Newspaper Featured Article Section Completed")
        
        # Parse Top News section
        logging.info("Parsing Punch Newspaper Top News Section In Progress")
        top_news_section = soup.find('div', class_='top-news')
        if top_news_section:
            for item in top_news_section.find_all('article'):
                article = {}
                
                title_elem = item.find('h2', class_='post-title')
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
        logging.info("Parsing Punch Newspaper Top News Section Completed")
        
        # Parse Metro Plus section
        logging.info("Parsing Punch Newspaper Metro Plus Section In Progress")
        metro_section = soup.find('div', class_='col-lg-12 nine-post')
        if metro_section:
            header = metro_section.find('h2', class_='header-title')
            section_name = header.find('a').get_text(strip=True) if header and header.find('a') else 'Metro Plus'
            
            # Parse main article
            logging.info("Parsing Punch Newspaper Metro Plus Section, Main article in Progress")
            main_article = metro_section.find('div', class_='news-widget-1')
            if main_article:
                article = {}
                
                title_elem = main_article.find('h2', class_='post-title')
                if title_elem and title_elem.find('a'):
                    article['title'] = title_elem.find('a').get_text(strip=True)
                    article['news_url'] = title_elem.find('a').get('href', '')
                
                
                img_elem = main_article.find('img')
                if img_elem:
                    article['image_url'] = img_elem.get('data-src') or img_elem.get('src', '')
                if not img_elem:
                    article['image_url'] = None
                if article.get('title'):
                    parsed_data['articles'].append(article)
            
            # Parse side articles
            logging.info("Parsing Punch Newspaper Metro Plus Section Side Articles In Progress")
            side_articles = metro_section.find_all('div', class_='news-widget-2')
            for widget in side_articles:
                for item in widget.find_all('article'):
                    article = {}
                    
                    title_elem = item.find('h2', class_='post-title')
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
        logging.info("Parsing Punch Newspaper Metro Plus Section Completed")
        
        # Parse other sections (Business, Politics, Sports, etc.)
        logging.info("Parsing Punch Newspaper Other Sections In Progress")
        sections = soup.find_all('div', class_='news-section-three')
        for section in sections:
            header = section.find('h2', class_='header-title')
            section_name = header.find('a').get_text(strip=True) if header and header.find('a') else 'General'
            
            for item in section.find_all('article'):
                article = {}
                
                title_elem = item.find('h2', class_='post-title')
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
        logging.info("Parsing Punch Newspaper Other Sections Completed")
        
        # Remove duplicates based on title
        logging.info("Removing Duplicates In Progress")
        seen_titles = set()
        unique_articles = []
        for article in parsed_data['articles']:
            if article['title'] not in seen_titles:
                seen_titles.add(article['title'])
                unique_articles.append(article)
        
        parsed_data['articles'] = unique_articles
        parsed_data['total_articles'] = len(unique_articles)
        
        end = time.time()
        
        logging.info(f"Parsing Completed. Total Articles: {len(unique_articles)}. Time Taken: {end - start:.2f} seconds.")
        
        return parsed_data
        
    except Exception as e:
        logging.error(f"An Error Occurred When Parsing Punch Newspaper Data: {e}")
        return None


