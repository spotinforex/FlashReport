if __name__ == "__main__":
    from pathlib import Path 
    import json
    with open("guardian.txt", "r") as f:
        content = f.read()
    result = parse_guardian_news(content)
    path = Path.cwd()
    with open(f"guardian_result.json", "w") as w:
        json.dump(result, w, indent = 2)
    
