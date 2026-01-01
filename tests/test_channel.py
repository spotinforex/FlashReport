from pathlib import Path 
import json

if __name__=="__main__":
    path = Path.cwd()
    with open(f"{path}/channel2.txt", "r") as f:
         content = f.read()
    result = parse_channel_news(content)
    with open("channel_result.json", 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
