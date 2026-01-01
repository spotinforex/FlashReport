# Example usage
if __name__ == "__main__":
        from pathlib import Path
        import json
        # Read HTML file
        path = Path.cwd()
        html_file = (f"{path}/premuimtimes.txt")
    
    
        with open(html_file, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        # Parse the content
        result = parse_premium_times_news(html_content)
        with open("premuimtimesnews_result.json", "w") as f:
            json.dump(result,f, indent =2)
