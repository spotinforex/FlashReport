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
from pathlib import Path
import time


def parse_arise_news(html_content):
    """
    Parse Arise TV News website content and extract articles
    Args:
        html_content: Raw Html Data 
    Returns:
        parsed_data: Parsed Data in Json Format 
    """
    try:
        start = time.time()
        soup = BeautifulSoup(html_content, 'lxml')
        
        parsed_data = {
            'source': 'Arise TV News',
            'source_url': 'https://www.arise.tv',
            'scraped_at': datetime.now().isoformat(),
            'articles': []
        }
        
        # Parse Featured Article
        logging.info("Parsing Arise TV Featured Article Section In Progress")
        featured = soup.find('article', class_='snippet typography featured')
        if featured:
            article = {}
            
            title_elem = featured.find('h3')
            if title_elem and title_elem.find('a'):
                article['title'] = title_elem.find('a').get_text(strip=True)
                article['url'] = title_elem.find('a').get('href', '')
            
            img_elem = featured.find('img', class_='wp-post-image')
            if img_elem:
                article['image_url'] = img_elem.get('src', '')
            
            excerpt_elem = featured.find('div', class_='excerpt')
            if excerpt_elem:
                article['excerpt'] = excerpt_elem.get_text(strip=True)
            
            if article.get('title'):
                article['category'] = 'Featured'
                article['is_featured'] = True
                parsed_data['articles'].append(article)
        logging.info("Parsing Arise TV Featured Article Section Completed")
        
        # Parse Latest News section
        logging.info("Parsing Arise TV Latest News Section In Progress")
        latest_news_panel = soup.find('div', class_='panel')
        if latest_news_panel:
            header = latest_news_panel.find('h2', class_='panel-title')
            if header and 'LATEST NEWS' in header.get_text().upper():
                articles_section = latest_news_panel.find_all('article', class_='snippet')
                
                for item in articles_section:
                    article = {}
                    
                    title_elem = item.find('h3')
                    if title_elem and title_elem.find('a'):
                        article['title'] = title_elem.find('a').get_text(strip=True)
                        article['url'] = title_elem.find('a').get('href', '')
                    
                    img_elem = item.find('img', class_='wp-post-image')
                    if img_elem:
                        article['image_url'] = img_elem.get('src', '')
                    
                    if article.get('title'):
                        article['category'] = 'Latest News'
                        parsed_data['articles'].append(article)
        logging.info("Parsing Arise TV Latest News Section Completed")
        
        # Parse Global News section
        logging.info("Parsing Arise TV Global News Section In Progress")
        panels = soup.find_all('div', class_='panel')
        for panel in panels:
            header = panel.find('h2', class_='panel-title')
            if header:
                section_link = header.find('a')
                section_name = section_link.get_text(strip=True) if section_link else header.get_text(strip=True)
                
                # Skip if not one of the main sections
                if section_name.upper() not in ['GLOBAL', 'AFRICA', 'POLITICS', 'BUSINESS', 'SPORT', 'ENTERTAINMENT']:
                    continue
                
                logging.info(f"Parsing Arise TV {section_name} Section In Progress")
                
                articles_section = panel.find_all('article', class_='snippet')
                for item in articles_section:
                    article = {}
                    
                    title_elem = item.find('h3')
                    if title_elem and title_elem.find('a'):
                        article['title'] = title_elem.find('a').get_text(strip=True)
                        article['url'] = title_elem.find('a').get('href', '')
                    
                    img_elem = item.find('img', class_='wp-post-image')
                    if img_elem:
                        article['image_url'] = img_elem.get('src', '')
                    
                    if article.get('title'):
                        article['category'] = section_name
                        parsed_data['articles'].append(article)
                
                logging.info(f"Parsing Arise TV {section_name} Section Completed")
        
        # Parse Videos section
        logging.info("Parsing Arise TV Videos Section In Progress")
        video_gallery = soup.find('div', class_='epyt-gallery')
        if video_gallery:
            video_thumbs = video_gallery.find_all('div', class_='epyt-gallery-thumb')
            
            for thumb in video_thumbs:
                article = {}
                
                # Get video ID
                video_id = thumb.get('data-videoid', '')
                if video_id:
                    article['url'] = f'https://www.youtube.com/watch?v={video_id}'
                    article['video_id'] = video_id
                
                # Get title
                title_elem = thumb.find('div', class_='epyt-gallery-title')
                if title_elem:
                    article['title'] = title_elem.get_text(strip=True)
                
                # Get thumbnail
                img_box = thumb.find('div', class_='epyt-gallery-img')
                if img_box:
                    style = img_box.get('style', '')
                    if 'url(' in style:
                        start_idx = style.find('url(') + 4
                        end_idx = style.find(')', start_idx)
                        article['image_url'] = style[start_idx:end_idx]
                
                if article.get('title'):
                    article['category'] = 'Videos'
                    article['content_type'] = 'video'
                    parsed_data['articles'].append(article)
        logging.info("Parsing Arise TV Videos Section Completed")
        
        # Parse Popular Articles section
        logging.info("Parsing Arise TV Popular Articles Section In Progress")
        sidebar_sections = soup.find_all('div', class_='arise-sidebar')
        for sidebar in sidebar_sections:
            popular_section = sidebar.find('h2', class_='panel-title')
            if popular_section and 'POPULAR' in popular_section.get_text().upper():
                parent_panel = popular_section.find_parent('div', class_='panel')
                if parent_panel:
                    articles_section = parent_panel.find_all('article', class_='snippet')
                    
                    for item in articles_section:
                        article = {}
                        
                        title_elem = item.find('h3')
                        if title_elem and title_elem.find('a'):
                            article['title'] = title_elem.find('a').get_text(strip=True)
                            article['url'] = title_elem.find('a').get('href', '')
                        
                        img_elem = item.find('img', class_='wp-post-image')
                        if img_elem:
                            article['image_url'] = img_elem.get('src', '')
                        
                        if article.get('title'):
                            article['category'] = 'Popular'
                            article['is_popular'] = True
                            parsed_data['articles'].append(article)
        logging.info("Parsing Arise TV Popular Articles Section Completed")
        
        # Parse Top Stories section
        logging.info("Parsing Arise TV Top Stories Section In Progress")
        for sidebar in sidebar_sections:
            top_stories_section = sidebar.find('h2', class_='panel-title')
            if top_stories_section:
                section_link = top_stories_section.find('a')
                if section_link and 'Top Stories' in section_link.get_text():
                    parent_panel = top_stories_section.find_parent('div', class_='panel')
                    if parent_panel:
                        articles_section = parent_panel.find_all('article', class_='snippet')
                        
                        for item in articles_section:
                            article = {}
                            
                            title_elem = item.find('h3')
                            if title_elem and title_elem.find('a'):
                                article['title'] = title_elem.find('a').get_text(strip=True)
                                article['url'] = title_elem.find('a').get('href', '')
                            
                            img_elem = item.find('img', class_='wp-post-image')
                            if img_elem:
                                article['image_url'] = img_elem.get('src', '')
                            
                            if article.get('title'):
                                article['category'] = 'Top Stories'
                                article['is_top_story'] = True
                                parsed_data['articles'].append(article)
        logging.info("Parsing Arise TV Top Stories Section Completed")
        
        # Parse Exclusives section
        logging.info("Parsing Arise TV Exclusives Section In Progress")
        for sidebar in sidebar_sections:
            exclusives_section = sidebar.find('h2', class_='panel-title')
            if exclusives_section:
                section_link = exclusives_section.find('a')
                if section_link and 'EXCLUSIVES' in section_link.get_text().upper():
                    parent_panel = exclusives_section.find_parent('div', class_='panel')
                    if parent_panel:
                        articles_section = parent_panel.find_all('article', class_='snippet')
                        
                        for item in articles_section:
                            article = {}
                            
                            title_elem = item.find('h3')
                            if title_elem and title_elem.find('a'):
                                article['title'] = title_elem.find('a').get_text(strip=True)
                                article['url'] = title_elem.find('a').get('href', '')
                            
                            img_elem = item.find('img', class_='wp-post-image')
                            if img_elem:
                                article['image_url'] = img_elem.get('src', '')
                            
                            if article.get('title'):
                                article['category'] = 'Exclusives'
                                article['is_exclusive'] = True
                                parsed_data['articles'].append(article)
        logging.info("Parsing Arise TV Exclusives Section Completed")
        
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
        logging.error(f"An Error Occurred When Parsing Arise TV News Data: {e}")
        return None
