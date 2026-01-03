if __name__ == "__main__":
    from pathlib import Path
    import json
    path = Path.cwd()
    with open(f"{path}/arise.txt", "r") as f:
        content = f.read()
    result = parse_arise_news(content)
    with open(f"arise_result.json", "w") as w:
        json.dump(result, w, indent = 2)
