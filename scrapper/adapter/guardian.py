import logging
import sys
from bs4 import BeautifulSoup
from datetime import datetime
import time

logger = logging.getLogger("runner")
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

def parse_guardian_news(html_content):
    """
    Parse The Guardian Nigeria website content and extract articles
    Args:
        html_content: Raw HTML Data 
    Returns:
        parsed_data: Parsed Data in JSON Format 
    """
    try:
        start = time.time()
        soup = BeautifulSoup(html_content, 'lxml')
        
        parsed_data = {
            'source': 'The Guardian Nigeria',
            'source_url': 'https://guardian.ng',
            'scraped_at': datetime.now().isoformat(),
            'articles': []
        }
        
        # Parse Breaking News Widget
        logging.info("Parsing Guardian Breaking News Section In Progress")
        breaking_news = soup.find('div', class_='breaking-news-widget')
        if breaking_news:
            article = {}
            link_elem = breaking_news.find('a')
            if link_elem:
                article['title'] = link_elem.get_text(strip=True)
                article['news_url'] = link_elem.get('href', '')
                parsed_data['articles'].append(article)
        logging.info("Parsing Guardian Breaking News Section Completed")
        
        # Parse Top Section (Main Featured Article)
        logging.info("Parsing Guardian Top Section In Progress")
        top_section = soup.find('section', class_='top-section')
        if top_section:
            # Main top news article
            top_article = top_section.find('article', class_='top-news-article')
            if top_article:
                article = {}
                
                title_elem = top_article.find('h2', class_='post-title')
                if title_elem and title_elem.find('a'):
                    article['title'] = title_elem.find('a').get_text(strip=True)
                    article['news_url'] = title_elem.find('a').get('href', '')
                
                img_elem = top_article.find('img')
                if img_elem:
                    article['image_url'] = img_elem.get('src', '')
                    parsed_data['articles'].append(article)
                if not img_elem:
                    article['image_url'] = None
            
            # Side articles in top section
            side_widgets = top_section.find_all('article', class_='top-section-news-widget-one')
            for widget in side_widgets:
                article = {}
                
                title_elem = widget.find('h2', class_='post-title')
                if title_elem and title_elem.find('a'):
                    article['title'] = title_elem.find('a').get_text(strip=True)
                    article['news_url'] = title_elem.find('a').get('href', '')
                
                img_elem = widget.find('img')
                if img_elem:
                    article['image_url'] = img_elem.get('src') or img_elem.get('data-src', '')
                if not img_elem:
                    article['image_url'] = None
                
                if article.get('title'):
                    parsed_data['articles'].append(article)
        logging.info("Parsing Guardian Top Section Completed")
        
        # Parse Latest News Section
        logging.info("Parsing Guardian Latest News Section In Progress")
        latest_news_div = soup.find('div', class_='latest-news-top-section')
        if latest_news_div:
            for item in latest_news_div.find_all('article'):
                article = {}
                
                title_elem = item.find('h2')
                if title_elem and title_elem.find('a'):
                    article['title'] = title_elem.find('a').get_text(strip=True)
                    article['news_url'] = title_elem.find('a').get('href', '')
                    article['image_url'] = None
                
                # Extract category and time
                meta_elem = item.find('div', class_='post-meta')
                if meta_elem:
                    spans = meta_elem.find_all('span')
                    if len(spans) >= 2:
                        article['published_at'] = spans[1].get_text(strip=True)
                
                if article.get('title'):
                    parsed_data['articles'].append(article)
        logging.info("Parsing Guardian Latest News Section Completed")
        
        # Parse News Group One Sections (News, Metro, Business, Energy, Politics)
        logging.info("Parsing Guardian News Group One Sections In Progress")
        news_group_sections = soup.find_all('section', class_='news-group-one')
        for section in news_group_sections:
            header = section.find('header')
            if header:
                section_link = header.find('h2')
                section_name = section_link.find('a').get_text(strip=True) if section_link and section_link.find('a') else 'General'
                
                # Parse news-type-one articles (main articles with excerpts)
                for item in section.find_all('article', class_='news-type-one'):
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
                
                # Parse news-type-two articles (medium size articles)
                for item in section.find_all('article', class_='news-type-two'):
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
                    
                    date_elem = item.find('span', class_='post-date')
                    if date_elem:
                        article['published_at'] = date_elem.get_text(strip=True)
                    
                    if article.get('title'):
                        parsed_data['articles'].append(article)
                
                # Parse news-type-three articles (small list articles)
                for item in section.find_all('article', class_='news-type-three'):
                    article = {}
                    
                    title_elem = item.find('h2', class_='post-title')
                    if title_elem and title_elem.find('a'):
                        article['title'] = title_elem.find('a').get_text(strip=True)
                        article['news_url'] = title_elem.find('a').get('href', '')
                        
                        article['image_url'] = None
                    
                    date_elem = item.find('span', class_='post-date')
                    if date_elem:
                        article['published_at'] = date_elem.get_text(strip=True)
                    
                    if article.get('title'):
                        parsed_data['articles'].append(article)
        logging.info("Parsing Guardian News Group One Sections Completed")
        
        # Parse News Group Two Sections (Education, Editorial, Health)
        logging.info("Parsing Guardian News Group Two Sections In Progress")
        news_group_two = soup.find_all('section', class_='news-group-two')
        for section in news_group_two:
            header = section.find('header')
            if header:
                section_link = header.find('h2')
                section_name = section_link.find('a').get_text(strip=True) if section_link and section_link.find('a') else 'General'
                
                # Parse news-type-four articles
                for item in section.find_all('article', class_='news-type-four'):
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
                    
                    meta_elem = item.find('p', class_='post-meta')
                    if meta_elem:
                        article['published_at'] = meta_elem.get_text(strip=True)
                    
                    if article.get('title'):
                        parsed_data['articles'].append(article)
        logging.info("Parsing Guardian News Group Two Sections Completed")
        
        # Parse News Group Three Sections (Sports, Technology, Opinions)
        logging.info("Parsing Guardian News Group Three Sections In Progress")
        news_group_three = soup.find_all('section', class_='news-group-three')
        for section in news_group_three:
            header = section.find('header')
            if header:
                section_link = header.find('h2')
                section_name = section_link.find('a').get_text(strip=True) if section_link and section_link.find('a') else 'General'
                
                # Parse news-type-five articles (large featured)
                for item in section.find_all('article', class_='news-type-five'):
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
                
                # Parse news-type-two in this section
                for item in section.find_all('article', class_='news-type-two'):
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
                    
                    date_elem = item.find('span', class_='post-date')
                    if date_elem:
                        article['published_at'] = date_elem.get_text(strip=True)
                    
                    if article.get('title'):
                        parsed_data['articles'].append(article)
        logging.info("Parsing Guardian News Group Three Sections Completed")
        
        # Parse Guardian Life Section
        logging.info("Parsing Guardian Life Section In Progress")
        guardian_life = soup.find('section', class_='guardian-life-section')
        if guardian_life:
            for item in guardian_life.find_all('article'):
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
        logging.info("Parsing Guardian Life Section Completed")
        
        # Parse Guardian Woman Section
        logging.info("Parsing Guardian Woman Section In Progress")
        guardian_woman = soup.find('section', class_='gwoman-section')
        if guardian_woman:
            for item in guardian_woman.find_all('article'):
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
                
                date_elem = item.find('p', class_='post-date')
                if date_elem:
                    article['published_at'] = date_elem.get_text(strip=True)
                
                if article.get('title'):
                    parsed_data['articles'].append(article)
        logging.info("Parsing Guardian Woman Section Completed")
        
        # Parse Guardian Angels Section
        logging.info("Parsing Guardian Angels Section In Progress")
        guardian_angels = soup.find('section', class_='gangels-section')
        if guardian_angels:
            for item in guardian_angels.find_all('article'):
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
                
                date_elem = item.find('span', class_='post-date')
                if date_elem:
                    article['published_at'] = date_elem.get_text(strip=True)
                
                if article.get('title'):
                    parsed_data['articles'].append(article)
        logging.info("Parsing Guardian Angels Section Completed")
        
        # Parse Guardian TV Section
        logging.info("Parsing Guardian TV Section In Progress")
        gtv_section = soup.find('section', class_='gtv-section')
        if gtv_section:
            for item in gtv_section.find_all('article'):
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
                
                date_elem = item.find('p', class_='post-date')
                if date_elem:
                    article['published_at'] = date_elem.get_text(strip=True)
                
                if article.get('title'):
                    parsed_data['articles'].append(article)
        logging.info("Parsing Guardian TV Section Completed")
        
        # Parse Marie Claire Section
        logging.info("Parsing Marie Claire Section In Progress")
        marie_claire = soup.find('section', class_='marie-claire-section')
        if marie_claire:
            for item in marie_claire.find_all('article'):
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
                
                date_elem = item.find('span', class_='post-date')
                if date_elem:
                    article['published_at'] = date_elem.get_text(strip=True)
                
                if article.get('title'):
                    parsed_data['articles'].append(article)
        logging.info("Parsing Marie Claire Section Completed")
        
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
        logging.error(f"An Error Occurred When Parsing Guardian Nigeria Data: {e}")
        return None
        
