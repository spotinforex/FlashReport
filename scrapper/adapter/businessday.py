import logging
import sys
import time
from bs4 import BeautifulSoup
from datetime import datetime

logger = logging.getLogger("runner")
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

def parse_businessday_news(html_content):
    """
    Parse BusinessDay website content and extract articles
    Args:
        html_content: Raw HTML Data 
    Returns:
        parsed_data: Parsed Data in JSON Format 
    """
    try:
        start = time.time()
        soup = BeautifulSoup(html_content, 'lxml')
        
        parsed_data = {
            'source': 'BusinessDay Nigeria',
            'source_url': 'https://businessday.ng',
            'scraped_at': datetime.now().isoformat(),
            'articles': []
        }
        
        # Parse Top Stories Section
        logging.info("Parsing BusinessDay Top Stories Section In Progress")
        top_stories_section = soup.find('div', class_='top_stories')
        if top_stories_section:
            articles = top_stories_section.find_all('article')
            for item in articles:
                article = {}
                
                title_elem = item.find('h2', class_='post-title')
                if title_elem and title_elem.find('a'):
                    article['title'] = title_elem.find('a').get_text(strip=True)
                    article['news_url'] = title_elem.find('a').get('href', '')
                
                time_elem = item.find('span', class_='time')
                if time_elem:
                    article['published_at'] = time_elem.get_text(strip=True)
                    article['image_url'] = None
                parsed_data['articles'].append(article)
        logging.info("Parsing BusinessDay Top Stories Section Completed")
        
        # Parse Main Featured Story
        logging.info("Parsing BusinessDay Featured Story Section In Progress")
        main_featured = soup.find('div', class_='main')
        if main_featured:
            item = main_featured.find('article')
            if item:
                article = {}
                
                title_elem = item.find('h2')
                if title_elem and title_elem.find('a'):
                    article['title'] = title_elem.find('a').get_text(strip=True)
                    article['news_url'] = title_elem.find('a').get('href', '')
                
                time_elem = item.find('span', class_='post-time')
                if time_elem:
                    article['published_at'] = time_elem.get_text(strip=True)
                
                img_elem = item.find('img')
                if img_elem:
                    article['image_url'] = img_elem.get('data-src') or img_elem.get('src', '')
                if not img_elem:
                    article['image_url'] = None
                parsed_data['articles'].append(article)
        logging.info("Parsing BusinessDay Featured Story Section Completed")
        
        # Parse Recent News Section
        logging.info("Parsing BusinessDay Recent News Section In Progress")
        recent_section = soup.find('div', class_='recent')
        if recent_section:
            articles = recent_section.find_all('article')
            for item in articles:
                article = {}
                
                title_elem = item.find('h2', class_='post-title')
                if title_elem and title_elem.find('a'):
                    article['title'] = title_elem.find('a').get_text(strip=True)
                    article['news_url'] = title_elem.find('a').get('href', '')
                
                time_elem = item.find('span', class_='time')
                if time_elem:
                    article['published_at'] = time_elem.get_text(strip=True)
                    article['image_url'] = None
                parsed_data['articles'].append(article)
        logging.info("Parsing BusinessDay Recent News Section Completed")
        
        # Parse Premium Section
        logging.info("Parsing BusinessDay Premium Section In Progress")
        premium_section = soup.find('div', class_='pro-section')
        if premium_section:
            articles = premium_section.find_all('article')
            for item in articles:
                article = {}
                
                title_elem = item.find('h2', class_='post-title')
                if title_elem and title_elem.find('a'):
                    article['title'] = title_elem.find('a').get_text(strip=True)
                    article['news_url'] = title_elem.find('a').get('href', '')
                
                date_elem = item.find('span', class_='post-date')
                if date_elem:
                    article['published_at'] = date_elem.get_text(strip=True)
                
                img_elem = item.find('img')
                if img_elem:
                    article['image_url'] = img_elem.get('data-src') or img_elem.get('src', '')
                if not img_elem:
                    article['image_url'] = None
                parsed_data['articles'].append(article)
        logging.info("Parsing BusinessDay Premium Section Completed")
        
        # Parse Other News Section
        logging.info("Parsing BusinessDay Other News Section In Progress")
        other_news_section = soup.find('div', class_='other-news-section')
        if other_news_section:
            articles = other_news_section.find_all('article')
            for item in articles:
                article = {}
                
                title_elem = item.find('h2', class_='post-title')
                if title_elem and title_elem.find('a'):
                    article['title'] = title_elem.find('a').get_text(strip=True)
                    article['news_url'] = title_elem.find('a').get('href', '')
                
                
                time_elem = item.find('span', class_='post-time')
                if time_elem:
                    article['published_at'] = time_elem.get_text(strip=True)
                
                img_elem = item.find('img')
                if img_elem:
                    article['image_url'] = img_elem.get('data-src') or img_elem.get('src', '')
                if not img_elem:
                    article['image_url'] = None
                parsed_data['articles'].append(article)
        logging.info("Parsing BusinessDay Other News Section Completed")
        
        # Parse Columnists Section
        logging.info("Parsing BusinessDay Columnists Section In Progress")
        columnist_section = soup.find('div', class_='columnist-news')
        if columnist_section:
            articles = columnist_section.find_all('article')
            for item in articles:
                article = {}
                
                title_elem = item.find('h2', class_='post-title')
                if title_elem and title_elem.find('a'):
                    article['title'] = title_elem.find('a').get_text(strip=True)
                    article['news_url'] = title_elem.find('a').get('href', '')
                
                time_elem = item.find('span', class_='post-time')
                if time_elem:
                    article['published_at'] = time_elem.get_text(strip=True)
                    article['image_url'] = None
                parsed_data['articles'].append(article)
        logging.info("Parsing BusinessDay Columnists Section Completed")
        
        # Parse Opinion News Section
        logging.info("Parsing BusinessDay Opinion Section In Progress")
        opinion_section = soup.find('div', class_='opinion-news')
        if opinion_section:
            articles = opinion_section.find_all('article')
            for item in articles:
                article = {}
                
                title_elem = item.find('p', class_='post-title')
                if title_elem and title_elem.find('a'):
                    article['title'] = title_elem.find('a').get_text(strip=True)
                    article['news_url'] = title_elem.find('a').get('href', '')
                
                time_elem = item.find('span', class_='post-time')
                if time_elem:
                    article['published_at'] = time_elem.get_text(strip=True)
                    article['image_url'] = None
                parsed_data['articles'].append(article)
        logging.info("Parsing BusinessDay Opinion Section Completed")
        
        # Parse Section-wise News (Economy, Energy, Technology, Markets, etc.)
        logging.info("Parsing BusinessDay Section-specific News In Progress")
        sections = soup.find_all('section', class_='news-block-2')
        for section in sections:
            header = section.find('div', class_='section-heading')
            if header and header.find('a'):
                section_name = header.find('span').get_text(strip=True) if header.find('span') else 'General'
                
                articles_list = section.find_all('article')
                for item in articles_list:
                    article = {}
                    
                    title_elem = item.find('h2', class_='post-title')
                    if title_elem and title_elem.find('a'):
                        article['title'] = title_elem.find('a').get_text(strip=True)
                        article['news_url'] = title_elem.find('a').get('href', '')
                    
                   
                    date_elem = item.find('span', class_='post-date') or item.find('span', class_='post-time')
                    if date_elem:
                        article['published_at'] = date_elem.get_text(strip=True)
                    
                    img_elem = item.find('img')
                    if img_elem:
                        article['image_url'] = img_elem.get('data-src') or img_elem.get('src', '')
                    if not img_elem:
                        article['image_url'] = None
                    parsed_data['articles'].append(article)
        logging.info("Parsing BusinessDay Section-specific News Completed")
        
        # Parse Partner Content Section
        logging.info("Parsing BusinessDay Partner Content Section In Progress")
        partner_blocks = soup.find_all('section', class_='news-block-3')
        for block in partner_blocks:
            header = block.find('div', class_='section-heading')
            section_name = 'Partner Content'
            if header and header.find('a') and header.find('span'):
                section_name = header.find('span').get_text(strip=True)
            
            articles_list = block.find_all('article')
            for item in articles_list:
                article = {}
                
                title_elem = item.find('h2', class_='post-title')
                if title_elem and title_elem.find('a'):
                    article['title'] = title_elem.find('a').get_text(strip=True)
                    article['news_url'] = title_elem.find('a').get('href', '')
                
                date_elem = item.find('span', class_='post-date')
                if date_elem:
                    article['published_at'] = date_elem.get_text(strip=True)
                
                img_elem = item.find('img')
                if img_elem:
                    article['image_url'] = img_elem.get('data-src') or img_elem.get('src', '')
                if not img_elem:
                    article['image_url'] = None
                parsed_data['articles'].append(article)
        logging.info("Parsing BusinessDay Partner Content Section Completed")
        
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
        logging.error(f"An Error Occurred When Parsing BusinessDay Data: {e}")
        return None

