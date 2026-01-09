from pathlib import Path 
import json

import json
from pathlib import Path

if __name__ == "__main__":
    path = Path.cwd()

    with open(path / "businessday_news.json", "r", encoding="utf-8") as f:
        content = json.load(f)   # ← THIS is the fix

    result = preprocessor(content)

    with open("preprocessor_result.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

