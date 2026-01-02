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
                    article['url'] = title_elem.find('a').get('href', '')
                
                time_elem = item.find('span', class_='time')
                if time_elem:
                    article['date_posted'] = time_elem.get_text(strip=True)
                
                if article.get('title'):
                    article['section'] = 'Top Stories'
                    article['category'] = 'Top News'
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
                    article['url'] = title_elem.find('a').get('href', '')
                
                author_elem = item.find('span', class_='post-author')
                if author_elem and author_elem.find('a'):
                    article['author'] = author_elem.find('a').get_text(strip=True)
                
                time_elem = item.find('span', class_='post-time')
                if time_elem:
                    article['date_posted'] = time_elem.get_text(strip=True)
                
                excerpt_elem = item.find('p')
                if excerpt_elem:
                    article['excerpt'] = excerpt_elem.get_text(strip=True)
                
                img_elem = item.find('img')
                if img_elem:
                    article['image_url'] = img_elem.get('data-src') or img_elem.get('src', '')
                
                if article.get('title'):
                    article['is_featured'] = True
                    article['section'] = 'Featured'
                    article['category'] = 'Featured Story'
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
                    article['url'] = title_elem.find('a').get('href', '')
                
                time_elem = item.find('span', class_='time')
                if time_elem:
                    article['date_posted'] = time_elem.get_text(strip=True)
                
                if article.get('title'):
                    article['section'] = 'Recent'
                    article['category'] = 'Recent News'
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
                    article['url'] = title_elem.find('a').get('href', '')
                
                date_elem = item.find('span', class_='post-date')
                if date_elem:
                    article['date_posted'] = date_elem.get_text(strip=True)
                
                img_elem = item.find('img')
                if img_elem:
                    article['image_url'] = img_elem.get('data-src') or img_elem.get('src', '')
                
                if article.get('title'):
                    article['section'] = 'Premium'
                    article['category'] = 'Premium Content'
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
                    article['url'] = title_elem.find('a').get('href', '')
                
                author_elem = item.find('span', class_='post-author')
                if author_elem and author_elem.find('a'):
                    article['author'] = author_elem.find('a').get_text(strip=True)
                
                time_elem = item.find('span', class_='post-time')
                if time_elem:
                    article['date_posted'] = time_elem.get_text(strip=True)
                
                excerpt_elem = item.find('p', class_='post-excerpt')
                if excerpt_elem:
                    article['excerpt'] = excerpt_elem.get_text(strip=True)
                
                category_elem = item.find('span', class_='category')
                if category_elem and category_elem.find('a'):
                    article['category'] = category_elem.find('a').get_text(strip=True)
                
                img_elem = item.find('img')
                if img_elem:
                    article['image_url'] = img_elem.get('data-src') or img_elem.get('src', '')
                
                if article.get('title'):
                    article['section'] = 'Other News'
                    if not article.get('category'):
                        article['category'] = 'News'
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
                    article['url'] = title_elem.find('a').get('href', '')
                
                author_elem = item.find('span', class_='post-author')
                if author_elem and author_elem.find('a'):
                    article['author'] = author_elem.find('a').get_text(strip=True)
                
                time_elem = item.find('span', class_='post-time')
                if time_elem:
                    article['date_posted'] = time_elem.get_text(strip=True)
                
                img_elem = item.find('img')
                if img_elem:
                    article['author_image'] = img_elem.get('src', '')
                
                if article.get('title'):
                    article['section'] = 'Columnists'
                    article['category'] = 'Opinion'
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
                    article['url'] = title_elem.find('a').get('href', '')
                
                time_elem = item.find('span', class_='post-time')
                if time_elem:
                    article['date_posted'] = time_elem.get_text(strip=True)
                
                category_elem = item.find('span', class_='category')
                if category_elem and category_elem.find('a'):
                    article['category'] = category_elem.find('a').get_text(strip=True)
                
                if article.get('title'):
                    article['section'] = 'Opinion'
                    if not article.get('category'):
                        article['category'] = 'Who Is Thinking For Nigeria'
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
                        article['url'] = title_elem.find('a').get('href', '')
                    
                    author_elem = item.find('span', class_='post-author')
                    if author_elem and author_elem.find('a'):
                        article['author'] = author_elem.find('a').get_text(strip=True)
                    
                    date_elem = item.find('span', class_='post-date') or item.find('span', class_='post-time')
                    if date_elem:
                        article['date_posted'] = date_elem.get_text(strip=True)
                    
                    excerpt_elem = item.find('p', class_='post-excerpt')
                    if excerpt_elem:
                        article['excerpt'] = excerpt_elem.get_text(strip=True)
                    
                    img_elem = item.find('img')
                    if img_elem:
                        article['image_url'] = img_elem.get('data-src') or img_elem.get('src', '')
                    
                    if article.get('title'):
                        article['section'] = section_name
                        article['category'] = section_name
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
                    article['url'] = title_elem.find('a').get('href', '')
                
                author_elem = item.find('span', class_='post-author')
                if author_elem and author_elem.find('a'):
                    article['author'] = author_elem.find('a').get_text(strip=True)
                
                date_elem = item.find('span', class_='post-date')
                if date_elem:
                    article['date_posted'] = date_elem.get_text(strip=True)
                
                img_elem = item.find('img')
                if img_elem:
                    article['image_url'] = img_elem.get('data-src') or img_elem.get('src', '')
                
                if article.get('title'):
                    article['section'] = section_name
                    article['category'] = section_name
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


# Example usage
if __name__ == "__main__":
    import json
    from pathlib import Path
    
    # Read the HTML file
    try:
        path = Path.cwd()
        with open(f'{path}/businessday.txt', 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        # Parse the content
        parsed_data = parse_businessday_news(html_content)
        
        if parsed_data:
            # Save to JSON file
            with open('businessday_news.json', 'w', encoding='utf-8') as f:
                json.dump(parsed_data, f, indent=2, ensure_ascii=False)
            
            logging.info(f"\nData saved to businessday_news.json")
            logging.info(f"Total articles scraped: {len(parsed_data['articles'])}")
            
            # Print sections summary
            sections_count = {}
            for article in parsed_data['articles']:
                section = article.get('section', 'Unknown')
                sections_count[section] = sections_count.get(section, 0) + 1
            
            logging.info("\n--- Articles by Section ---")
            for section, count in sorted(sections_count.items()):
                logging.info(f"{section}: {count} articles")
        else:
            logging.error("Failed to parse BusinessDay data")
            
    except FileNotFoundError:
        logging.error("businessday.html file not found. Please ensure the HTML file exists in the current directory.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
