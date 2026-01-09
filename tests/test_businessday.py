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
