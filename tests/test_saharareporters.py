if __name__ == "__main__":
    from pathlib import Path
    import json
    path = Path.cwd()
    with open(f"{path}/saharareporters.txt", "r") as f:
        content = f.read()
    result = parse_saharareporters_news(content)
    with open(f"{path}/saharareporters_news.json", "w") as w:
        json.dump(result, w, indent =2)
