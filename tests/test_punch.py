if __name__ == "__main__":
    from pathlib import Path
    import json 
    path = Path.cwd()
    with open(f"{path}/punch.txt", "r") as f:
        content = f.read()
    result = parse_punch_news(content)
    with open(f"punchnews_result.json", "w") as w:
        json.dump(result, w, indent = 2)
