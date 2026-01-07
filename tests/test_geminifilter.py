if __name__ == "__main__":
    from pathlib import Path
    path = Path.cwd()
    with open(f"{path}/ingest.txt", "r") as f:
        content = f.read()
    with open(f"{path}/Algorithm/system_instructions/filter_instructions.txt", "r") as w:
        instructions = w.read()
    prompt = f"{instructions} {content}"
    response = call_gemini(prompt)
    print(response)
