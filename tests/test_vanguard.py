# Example usage
if __name__ == "__main__":
    from pathlib import Path
    import json
    
    # Read HTML file
    path = Path.cwd()
    with open(f"{path}/vanguard.txt", 'r', encoding='utf-8') as f:
        html_content = f.read()
        
        # Parse the content
        result = parse_vanguard_news(html_content)
        
        if result:
            # Save to JSON file
            output_file = Path('vanguard_articles.json')
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
            
            logging.info(f"Data saved to {output_file}")
            
            # Print summary
            print(f"\n{'='*60}")
            print(f"SCRAPING SUMMARY")
            print(f"{'='*60}")
            print(f"Source: {result['source']}")
            print(f"Total Articles: {result['total_articles']}")
            print(f"Scraped At: {result['scraped_at']}")
            
            # Print sample by category
            categories = {}
            for article in result['articles']:
                cat = article.get('category', 'Unknown')
                categories[cat] = categories.get(cat, 0) + 1
            
            print(f"\nArticles by Category:")
            for cat, count in sorted(categories.items(), key=lambda x: x[1], reverse=True):
                print(f"  {cat}: {count}")
        else:
            logging.error(f"File {html_file} not found")
