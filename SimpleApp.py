# -*- coding: utf-8 -*-
from pyspark import SparkContext

sc = SparkContext("local", "Simple App")

import sys,os,bz2

path = "30/00/"
jsonpaths = []
for(dirpath,dirnames,files)in os.walk(path):
    for filename in files:
        filepath = os.path.join(dirpath, filename)
        zipfile = bz2.BZ2File(filepath) # open the file
        data = zipfile.read() # get the decompressed data
        # data es una cadena de texto con el contenido del archivo
        # en este ejemplo se guarda el contenido a otro fichero
        newfilepath = filepath[:-4] # assuming the filepath ends with .bz2
        jsonpaths.append(newfilepath)
        open(newfilepath, 'wb').write(data) # write a uncompressed file


rddPaths = sc.parallelize(jsonpaths)

import json
def parse_json(json_path):
    results = []
    with open('30/00/01.json','r') as f:
        lines = f.readlines()
        for line in lines:
            j = json.loads(line)
            if 'user' in j and j['user']['lang'] == 'es':
                user = j['user']['screen_name'].encode("utf-8")
                date = j['created_at'].encode("utf-8")
                text = j['text'].encode("utf-8")
                results.append((user, date, text))
    return results


print rddPaths.flatMap(parse_json).collect()