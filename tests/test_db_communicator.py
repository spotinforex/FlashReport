if __name__ == "__main__":
    import json
    data = get_all_events()
    with open("all_events.txt", "w") as f:
        json.dump(data,f, indent = 2,  default=json_serializer)
    data2 = search_events('flood')
    with open("search_event.txt", "w") as w:
        json.dump(data2,w,indent =2,  default=json_serializer)

from datetime import datetime, date

def json_serializer(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

if __name__ == "__main__":
    import json 
    d = get_all_events(limit=5)
    with open("event_response.json", "w") as f:
        json.dump(d, f, indent = 2, default = json_serializer)
