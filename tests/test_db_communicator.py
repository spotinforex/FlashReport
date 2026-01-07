if __name__ == "__main__":
    import json
    data = get_all_events()
    with open("all_events.txt", "w") as f:
        json.dump(data,f, indent = 2,  default=json_serializer)
    data2 = search_events('flood')
    with open("search_event.txt", "w") as w:
        json.dump(data2,w,indent =2,  default=json_serializer)

