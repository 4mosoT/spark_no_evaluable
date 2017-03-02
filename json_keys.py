import json, sys
with open('30/00/01.json','r') as f:
    lines = f.readlines()
    for line in lines:
        j = json.loads(line)
        if 'user' in j and j['user']['lang'] == 'es':
            user = j['user']['screen_name'].encode("utf-8")
            date = j['created_at'].encode("utf-8")
            text = j['text'].encode("utf-8")
            print (type(user), date, type(text))

