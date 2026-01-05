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


def parse_vanguard_news(html_content):
    """
    Parse Vanguard News website content and extract articles
    Args:
        html_content: Raw HTML Data 
    Returns:
        parsed_data: Parsed Data in JSON Format 
    """
    try:
        start = time.time()
        soup = BeautifulSoup(html_content, 'lxml')
        
        parsed_data = {
            'source': 'Vanguard Nigeria',
            'source_url': 'https://www.vanguardngr.com',
            'scraped_at': datetime.now().isoformat(),
            'articles': []
        }
        
        # Parse Latest News section (Timeline format)
        logging.info("Parsing Vanguard Latest News Section In Progress")
        latest_news_section = soup.find('div', class_='section-format-vertical_list')
        if latest_news_section:
            section_heading = latest_news_section.find('h2', class_='heading-title')
            section_name = 'Latest News'
            if section_heading:
                section_name = section_heading.get_text(strip=True)
            
            for item in latest_news_section.find_all('article', class_='entry'):
                article = {}
                
                # Extract title
                title_elem = item.find('h3', class_='entry-title')
                if title_elem and title_elem.find('a'):
                    article['title'] = title_elem.find('a').get_text(strip=True)
                    article['news_url'] = title_elem.find('a').get('href', '')
                
                # Extract image
                img_elem = item.find('img')
                if img_elem:
                    article['image_url'] = img_elem.get('data-lazy-src') or img_elem.get('src', '')
                if not img_elem:
                    article['image_url'] = None
                
                # Extract time
                time_elem = item.find('div', class_='entry-date')
                if time_elem:
                    article['published_at'] = time_elem.get_text(strip=True)
                
                if article.get('title'):
                    parsed_data['articles'].append(article)
        
        logging.info("Parsing Vanguard Latest News Section Completed")
        
        # Parse Featured Articles from category sections
        logging.info("Parsing Vanguard Featured Articles In Progress")
        featured_sections = soup.find_all('div', class_='section-content-featured')
        
        for featured in featured_sections:
            article = {}
            
            title_elem = featured.find('h3', class_='entry-title')
            if title_elem and title_elem.find('a'):
                article['title'] = title_elem.find('a').get_text(strip=True)
                article['news_url'] = title_elem.find('a').get('href', '')
            
            
            img_elem = featured.find('img')
            if img_elem:
                article['image_url'] = img_elem.get('data-lazy-src') or img_elem.get('src', '')
            if not img_elem:
                article['image_url'] = None
            
            if article.get('title'):
                parsed_data['articles'].append(article)
        
        logging.info("Parsing Vanguard Featured Articles Completed")
        
        # Parse Politics section
        logging.info("Parsing Vanguard Politics Section In Progress")
        politics_section = soup.find('section', class_='section-category-preview')
        if politics_section and politics_section.find('h2', class_='heading-title'):
            section_title = politics_section.find('h2', class_='heading-title')
            if 'Politics' in section_title.get_text():
                for item in politics_section.find_all('article', class_='entry-card'):
                    article = {}
                    
                    title_elem = item.find('h3', class_='entry-title')
                    if title_elem and title_elem.find('a'):
                        article['title'] = title_elem.find('a').get_text(strip=True)
                        article['news_url'] = title_elem.find('a').get('href', '')
                    
                    img_elem = item.find('img')
                    if img_elem:
                        article['image_url'] = img_elem.get('data-lazy-src') or img_elem.get('src', '')
                    if not img_elem:
                        article['image_url'] = None
                    
                    if article.get('title'):
                        parsed_data['articles'].append(article)
        
        logging.info("Parsing Vanguard Politics Section Completed")
        
        # Parse More News section
        logging.info("Parsing Vanguard More News Section In Progress")
        more_news_sections = soup.find_all('div', class_='section-fulwidth')
        
        for section in more_news_sections:
            header = section.find('h2', class_='heading-title')
            if not header:
                continue
            
            section_name = header.get_text(strip=True)
            
            # Parse featured article in section
            featured = section.find('div', class_='section-content-featured')
            if featured:
                article = {}
                
                title_elem = featured.find('h3', class_='entry-title')
                if title_elem and title_elem.find('a'):
                    article['title'] = title_elem.find('a').get_text(strip=True)
                    article['news_url'] = title_elem.find('a').get('href', '')
                
                img_elem = featured.find('img')
                if img_elem:
                    article['image_url'] = img_elem.get('data-lazy-src') or img_elem.get('src', '')
                if not img_elem:
                    article['image_url'] = None
                
                if article.get('title'):
                    parsed_data['articles'].append(article)
            
            # Parse list articles
            entry_list = section.find('div', class_='section-content-entry-list')
            if entry_list:
                for item in entry_list.find_all('article', class_='entry-card'):
                    article = {}
                    
                    title_elem = item.find('h3', class_='entry-title')
                    if title_elem and title_elem.find('a'):
                        article['title'] = title_elem.find('a').get_text(strip=True)
                        article['news_url'] = title_elem.find('a').get('href', '')
                    
                    img_elem = item.find('img')
                    if img_elem:
                        article['image_url'] = img_elem.get('data-lazy-src') or img_elem.get('src', '')
                    if not img_elem:
                        article['image_url'] = None
                    
                    if article.get('title'):
                        parsed_data['articles'].append(article)
        
        logging.info("Parsing Vanguard More News Section Completed")
        
        # Parse Metro section
        logging.info("Parsing Vanguard Metro Section In Progress")
        sections_with_names = soup.find_all('div', class_='section-fulwidth')
        
        for section in sections_with_names:
            header = section.find('h2', class_='heading-title')
            if header and 'Metro' in header.get_text():
                section_name = header.get_text(strip=True)
                
                for item in section.find_all('article', class_='entry-card'):
                    article = {}
                    
                    title_elem = item.find('h3', class_='entry-title')
                    if title_elem and title_elem.find('a'):
                        article['title'] = title_elem.find('a').get_text(strip=True)
                        article['news_url'] = title_elem.find('a').get('href', '')
                    
                    img_elem = item.find('img')
                    if img_elem:
                        article['image_url'] = img_elem.get('data-lazy-src') or img_elem.get('src', '')
                    if not img_elem:
                        article['image_url'] = None
                    
                    if article.get('title'):
                        parsed_data['articles'].append(article)
        
        logging.info("Parsing Vanguard Metro Section Completed")
        
        # Parse Columns section
        logging.info("Parsing Vanguard Columns Section In Progress")
        columns_section = soup.find('div', class_='section-format-horizontal_list')
        if columns_section:
            for item in columns_section.find_all('article', class_='entry'):
                article = {}
                
                title_elem = item.find('h3', class_='entry-title')
                if title_elem and title_elem.find('a'):
                    article['title'] = title_elem.find('a').get_text(strip=True)
                    article['news_url'] = title_elem.find('a').get('href', '')
                
                img_elem = item.find('img')
                if img_elem:
                    article['image_url'] = img_elem.get('data-lazy-src') or img_elem.get('src', '')
                if not img_elem:
                    article['image_url'] = None
                
                time_elem = item.find('div', class_='entry-date')
                if time_elem:
                    article['published_at'] = time_elem.get_text(strip=True)
                
                if article.get('title'):
                    parsed_data['articles'].append(article)
        
        logging.info("Parsing Vanguard Columns Section Completed")
        
        # Parse Entertainment section
        logging.info("Parsing Vanguard Entertainment Section In Progress")
        for section in sections_with_names:
            header = section.find('h2', class_='heading-title')
            if header and 'Entertainment' in header.get_text():
                for item in section.find_all('article', class_='entry-card'):
                    article = {}
                    
                    title_elem = item.find('h3', class_='entry-title')
                    if title_elem and title_elem.find('a'):
                        article['title'] = title_elem.find('a').get_text(strip=True)
                        article['news_url'] = title_elem.find('a').get('href', '')
                    
                    img_elem = item.find('img')
                    if img_elem:
                        article['image_url'] = img_elem.get('data-lazy-src') or img_elem.get('src', '')
                    if not img_elem:
                        article['image_url'] = None
                    
                    if article.get('title'):
                        parsed_data['articles'].append(article)
        
        logging.info("Parsing Vanguard Entertainment Section Completed")
        
        # Parse Sports section
        logging.info("Parsing Vanguard Sports Section In Progress")
        for section in sections_with_names:
            header = section.find('h2', class_='heading-title')
            if header and 'Sports' in header.get_text():
                for item in section.find_all('article', class_='entry-card'):
                    article = {}
                    
                    title_elem = item.find('h3', class_='entry-title')
                    if title_elem and title_elem.find('a'):
                        article['title'] = title_elem.find('a').get_text(strip=True)
                        article['news_url'] = title_elem.find('a').get('href', '')
                    
                    img_elem = item.find('img')
                    if img_elem:
                        article['image_url'] = img_elem.get('data-lazy-src') or img_elem.get('src', '')
                    if not img_elem:
                        article['image_url'] = None
                    
                    if article.get('title'):
                        parsed_data['articles'].append(article)
        
        logging.info("Parsing Vanguard Sports Section Completed")
        
        # Parse Business section
        logging.info("Parsing Vanguard Business Section In Progress")
        for section in sections_with_names:
            header = section.find('h2', class_='heading-title')
            if header and 'Business' in header.get_text():
                for item in section.find_all('article', class_='entry-card'):
                    article = {}
                    
                    title_elem = item.find('h3', class_='entry-title')
                    if title_elem and title_elem.find('a'):
                        article['title'] = title_elem.find('a').get_text(strip=True)
                        article['news_url'] = title_elem.find('a').get('href', '')
                    
                    img_elem = item.find('img')
                    if img_elem:
                        article['image_url'] = img_elem.get('data-lazy-src') or img_elem.get('src', '')
                    if not img_elem:
                        article['image_url'] = None
                    
                    if article.get('title'):
                        parsed_data['articles'].append(article)
        
        logging.info("Parsing Vanguard Business Section Completed")
        
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
        logging.error(f"An Error Occurred When Parsing Vanguard News Data: {e}")
        return None



