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


def parse_channel_news(html_content):
    """
    Parse Channels TV website content and extract articles
    Args:
        html_content: Raw HTML Data 
    Returns:
        parsed_data: Parsed Data in JSON Format 
    """
    try:
        start = time.time()
        soup = BeautifulSoup(html_content, 'lxml')
        
        parsed_data = {
            'source': 'Channels TV',
            'source_url': 'https://www.channelstv.com',
            'scraped_at': datetime.now().isoformat(),
            'articles': []
        }
        
        # Parse Lead/Featured Article
        logging.info("Parsing Channels TV Featured Article Section In Progress")
        lead_article = soup.find('div', class_='leading-article')
        if lead_article:
            article = {}
            
            title_elem = lead_article.find('h3', class_='post-title')
            if title_elem and title_elem.find('a'):
                article['title'] = title_elem.find('a').get_text(strip=True)
                article['url'] = title_elem.find('a').get('href', '')
            
            excerpt_elem = lead_article.find('div', class_='post-excerpt')
            if excerpt_elem and excerpt_elem.find('p'):
                article['excerpt'] = excerpt_elem.find('p').get_text(strip=True)
            
            # Get featured image
            main_thumbnail = soup.find('div', class_='main__article-thumbnail')
            if main_thumbnail:
                img = main_thumbnail.find('img')
                if img:
                    article['image_url'] = img.get('data-lazy-src') or img.get('src', '')
            
            if article.get('title'):
                article['category'] = 'Top Stories'
                article['is_featured'] = True
                parsed_data['articles'].append(article)
        
        logging.info("Parsing Channels TV Featured Article Section Completed")
        
        # Parse Top Stories section
        logging.info("Parsing Channels TV Top Stories Section In Progress")
        headlines_section = soup.find('section', class_='headlines')
        if headlines_section:
            logging.info("Parsing Channels TV Top Stories Section, Main Articles Extraction In Progress")
            articles_in_section = headlines_section.find_all('article', class_='post')
            
            for item in articles_in_section:
                article = {}
                
                # Extract title
                title_elem = item.find('h3', class_=['post-title', 'sumry-title'])
                if title_elem and title_elem.find('a'):
                    article['title'] = title_elem.find('a').get_text(strip=True)
                    article['url'] = title_elem.find('a').get('href', '')
                
                # Extract category
                category_elem = item.find('div', class_='post-category')
                if category_elem and category_elem.find('a'):
                    article['category'] = category_elem.find('a').get_text(strip=True)
                else:
                    article['category'] = 'Top Stories'
                
                # Extract excerpt
                excerpt_elem = item.find('div', class_='post-excerpt')
                if excerpt_elem and excerpt_elem.find('p'):
                    article['excerpt'] = excerpt_elem.find('p').get_text(strip=True)
                
                # Extract image
                img_elem = item.find('img')
                if img_elem:
                    article['image_url'] = img_elem.get('data-lazy-src') or img_elem.get('src', '')
                
                # Extract author
                author_elem = item.find('span', class_='post-author')
                if author_elem and author_elem.find('a'):
                    article['author'] = author_elem.find('a').get_text(strip=True)
                
                # Extract timestamp
                time_elem = item.find('div', class_='post_time')
                if time_elem and time_elem.find('span'):
                    article['time_posted'] = time_elem.find('span').get_text(strip=True)
                
                if article.get('title'):
                    parsed_data['articles'].append(article)
        
        logging.info("Parsing Channels TV Top Stories Section Completed")
        
        # Parse AFCON 2025 section
        logging.info("Parsing Channels TV AFCON 2025 Section In Progress")
        afcon_section = soup.find('section', class_='features')
        if afcon_section:
            header = afcon_section.find('h3')
            section_name = header.get_text(strip=True) if header else 'AFCON 2025'
            
            for item in afcon_section.find_all('article', class_='post'):
                article = {}
                
                title_elem = item.find('h3', class_=['post-title', 'sumry-title'])
                if title_elem and title_elem.find('a'):
                    article['title'] = title_elem.find('a').get_text(strip=True)
                    article['url'] = title_elem.find('a').get('href', '')
                
                excerpt_elem = item.find('div', class_='post-excerpt')
                if excerpt_elem and excerpt_elem.find('p'):
                    article['excerpt'] = excerpt_elem.find('p').get_text(strip=True)
                
                img_elem = item.find('img')
                if img_elem:
                    article['image_url'] = img_elem.get('data-lazy-src') or img_elem.get('src', '')
                
                if article.get('title'):
                    article['category'] = section_name
                    parsed_data['articles'].append(article)
        
        logging.info("Parsing Channels TV AFCON 2025 Section Completed")
        
        # Parse More Stories section
        logging.info("Parsing Channels TV More Stories Section In Progress")
        more_stories_section = soup.find('section', class_='more_stories')
        if more_stories_section:
            for item in more_stories_section.find_all('article', class_='post'):
                article = {}
                
                title_elem = item.find('h3', class_=['post-title', 'sumry-title'])
                if title_elem and title_elem.find('a'):
                    article['title'] = title_elem.find('a').get_text(strip=True)
                    article['url'] = title_elem.find('a').get('href', '')
                
                excerpt_elem = item.find('div', class_='post-excerpt')
                if excerpt_elem and excerpt_elem.find('p'):
                    article['excerpt'] = excerpt_elem.find('p').get_text(strip=True)
                
                img_elem = item.find('img')
                if img_elem:
                    article['image_url'] = img_elem.get('data-lazy-src') or img_elem.get('src', '')
                
                author_elem = item.find('span', class_='post-author')
                if author_elem and author_elem.find('a'):
                    article['author'] = author_elem.find('a').get_text(strip=True)
                
                if article.get('title'):
                    article['category'] = 'More Stories'
                    parsed_data['articles'].append(article)
        
        logging.info("Parsing Channels TV More Stories Section Completed")
        
        # Parse Latest Stories section
        logging.info("Parsing Channels TV Latest Stories Section In Progress")
        latest_stories = soup.find('div', class_='latest_stories')
        if latest_stories:
            for item in latest_stories.find_all('article', class_='post'):
                article = {}
                
                title_elem = item.find('h3', class_=['post-title', 'sumry-title'])
                if title_elem and title_elem.find('a'):
                    article['title'] = title_elem.find('a').get_text(strip=True)
                    article['url'] = title_elem.find('a').get('href', '')
                
                img_elem = item.find('img')
                if img_elem:
                    article['image_url'] = img_elem.get('data-lazy-src') or img_elem.get('src', '')
                
                time_elem = item.find('div', class_='post_time')
                if time_elem and time_elem.find('span'):
                    article['time_posted'] = time_elem.find('span').get_text(strip=True)
                
                if article.get('title'):
                    article['category'] = 'Latest Stories'
                    parsed_data['articles'].append(article)
        
        logging.info("Parsing Channels TV Latest Stories Section Completed")
        
        # Parse Sports section
        logging.info("Parsing Channels TV Sports Section In Progress")
        sports_section = soup.find('section', class_='sports')
        if sports_section:
            for item in sports_section.find_all('article', class_='post'):
                article = {}
                
                title_elem = item.find('h3', class_=['post-title', 'sumry-title'])
                if title_elem and title_elem.find('a'):
                    article['title'] = title_elem.find('a').get_text(strip=True)
                    article['url'] = title_elem.find('a').get('href', '')
                
                excerpt_elem = item.find('div', class_='post-excerpt')
                if excerpt_elem and excerpt_elem.find('p'):
                    article['excerpt'] = excerpt_elem.find('p').get_text(strip=True)
                
                img_elem = item.find('img')
                if img_elem:
                    article['image_url'] = img_elem.get('data-lazy-src') or img_elem.get('src', '')
                
                author_elem = item.find('span', class_='post-author')
                if author_elem and author_elem.find('a'):
                    article['author'] = author_elem.find('a').get_text(strip=True)
                
                if article.get('title'):
                    article['category'] = 'Sports'
                    parsed_data['articles'].append(article)
        
        logging.info("Parsing Channels TV Sports Section Completed")
        
        # Parse Politics section
        logging.info("Parsing Channels TV Politics Section In Progress")
        politics_section = soup.find('section', class_='politics')
        if politics_section:
            for item in politics_section.find_all('article', class_='post'):
                article = {}
                
                title_elem = item.find('h3', class_=['post-title', 'sumry-title'])
                if title_elem and title_elem.find('a'):
                    article['title'] = title_elem.find('a').get_text(strip=True)
                    article['url'] = title_elem.find('a').get('href', '')
                
                excerpt_elem = item.find('div', class_='post-excerpt')
                if excerpt_elem and excerpt_elem.find('p'):
                    article['excerpt'] = excerpt_elem.find('p').get_text(strip=True)
                
                img_elem = item.find('img')
                if img_elem:
                    article['image_url'] = img_elem.get('data-lazy-src') or img_elem.get('src', '')
                
                category_elem = item.find('div', class_='post-category')
                category = 'Politics'
                if category_elem and category_elem.find('a'):
                    category = category_elem.find('a').get_text(strip=True)
                
                if article.get('title'):
                    article['category'] = category
                    parsed_data['articles'].append(article)
        
        logging.info("Parsing Channels TV Politics Section Completed")
        
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
        logging.error(f"An Error Occurred When Parsing Channels TV Data: {e}")
        return None
