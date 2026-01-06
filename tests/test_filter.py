if __name__ == "__main__":
    from scrapper.database import Database

    database = Database()
    query = "SELECT * FROM parsed_articles;"
    rows = database.fetch_all(query)

    results = []

    for row in rows:
        msg = row[2]
        result = ingest_message(msg)

        if result:
            results.append((result, msg))

    # write to file
    with open("ingest.txt", "w", encoding="utf-8") as f:
        for result, msg in results:
            f.write(f"{result}: {msg}\n")

    print(len(results))
