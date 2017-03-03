# -*- coding: utf-8 -*-
from pyspark import SparkContext

sc = SparkContext("local", "Simple App")

import sys, os, bz2, json, re

path = "30/00/"
jsonpaths = []
for (dirpath, dirnames, files) in os.walk(path):
    for filename in files:
        filepath = os.path.join(dirpath, filename)
        zipfile = bz2.BZ2File(filepath)  # open the file
        data = zipfile.read()  # get the decompressed data
        # data es una cadena de texto con el contenido del archivo
        # en este ejemplo se guarda el contenido a otro fichero
        newfilepath = filepath[:-4]  # assuming the filepath ends with .bz2
        jsonpaths.append(newfilepath)
        open(newfilepath, 'wb').write(data)  # write a uncompressed file

rddPaths = sc.parallelize(jsonpaths)


def parse_json(json_path):
    results = []
    with open(json_path, 'r') as f:
        lines = f.readlines()
        for line in lines:
            j = json.loads(line)
            if 'user' in j and j['user']['lang'] == 'es':
                user = j['user']['screen_name'].encode("utf-8")
                date = j['created_at'].encode("utf-8")
                text = j['text']
                results.append((user, date, text))
    return results


rddTweets = rddPaths.flatMap(parse_json)


# print rddTweets.filter(lambda (usuario,fecha,tweet): 'antoniomadrian' in usuario).collect()



# usuarioMax = rddTweets.map(lambda x: (x[0],1)).reduceByKey(lambda c1,c2: c1+c2).max(lambda (a,b):b)
# print "Usuario mas tweets: ", usuarioMax


# def quitaNoAlfa(s):
# return re.sub(r'([^\s\wñáéíóú]|_)+', '', s.lower().encode('utf-8'))

# stopwords=[u'como', u'pero', u'o', u'al', u'mas', u'esta', u'le', u'cuando', u'eso', u'su', u'porque', u'd', u'del', u'los', u'mi', u'si', u'las', u'una', u'q', u'ya', u'yo', u'tu', u'el', u'ella', u'a', u'ante', u'bajo', u'cabe', u'con', u'contra', u'de', u'desde', u'en', u'entre', u'hacia', u'hasta', u'para', u'por', u'segun', u'sin', u'so', u'sobre', u'tras', u'que', u'la', u'no', u'y', u'el', u'me', u'es', u'te', u'se', u'un', u'lo']


# rddPalabras=rddTweets.flatMap(lambda x: quitaNoAlfa(x[2]).split(' ')).filter(lambda x: len(x)>0)
# diezPalabrasMasUsadas=rddPalabras.filter(lambda x: x not in stopwords).map(lambda x: (x,1)).reduceByKey(lambda c1,c2: c1+c2).takeOrdered(10, lambda (p,c):-c)


# print "Palabras más usadas", diezPalabrasMasUsadas


def quitaNoAlfaOHash(s):
    return re.sub(r'([^\s\w#áéíóúñ]|_)+', '', s.lower())


def sacaHashtags(tweet):
    hashtags = tweet.split('#')[1:]
    return map(lambda x: x if ' ' not in x else x.partition(' ')[0], hashtags)


rddHashtags = rddTweets.flatMap(lambda x: quitaNoAlfaOHash(x[2]).split(' ')).filter(
    lambda x: (len(x) > 1) and x[0] == '#')
rddAlcanceHashtags = rddHashtags.map(lambda x: (x[1:], 1)).reduceByKey(lambda c1, c2: c1 + c2)
# print "Hashtag mas usado: ",rddAlcanceHashtags.takeOrdered(3, lambda x: -x[1])

# rddLower = rddTweets.map(lambda (usuario,fecha,tweet): (usuario,fecha,tweet.lower())).filter(lambda (usuario,fecha,tweet): '#ff' in tweet)
# print "Tweets con el hashtag mas usado: ", rddLower.collect()

# rddConHashtag=rddTweets.filter(lambda x: '#' in x[2])

# rddHashtagsUsuario=rddConHashtag.map(lambda (usuario, fecha, tweet): (usuario,sacaHashtags(tweet)))

# rddParesHashtagUsuario=rddHashtagsUsuario.flatMap(lambda (usuario, hashtags): map(lambda h: (h,usuario),hashtags))

# rddAlcanceUsuarios=rddParesHashtagUsuario.join(rddAlcanceHashtags)

# rddAlcanceUsuarios=rddAlcanceUsuarios.map(lambda (h,(u,f)): (u,f)).reduceByKey(lambda p1,p2: p1+p2)

# print "Usuarios que mas hashtags usa: ",rddAlcanceUsuarios.takeOrdered(10, lambda x: -x[1])

# rddAlcanceUsuarios=rddParesHashtagUsuario.join(rddAlcanceHashtags.filter(lambda x: x[1]==1))
# rddAlcanceUsuarios=rddAlcanceUsuarios.map(lambda (h,(u,f)): (u,f)).reduceByKey(lambda p1,p2: p1+p2)

# print "Usuarios con mayor cantidad de hasthags inutiles: ",rddAlcanceUsuarios.takeOrdered(3, lambda x: -x[1])

import time
import datetime


def fechaATimestamp(f):
    return time.mktime(datetime.datetime.strptime(f, "%a %b %d %H:%M:%S +0000 %Y").timetuple())


def creaTuplasConHashtags((usuario, fecha, tweet)):
    res = []
    palabras = quitaNoAlfaOHash(tweet).split()
    for p in palabras:
        if len(p) > 1 and p[0] == '#':
            res.append((p[1:], (fechaATimestamp(fecha), usuario)))
    return res


rddHashtagsConUsuarioYFecha = rddTweets.flatMap(creaTuplasConHashtags)

# rddHashtagsConUsuarioYFecha=rddTweets.flatMap(lambda (u,f,t): map(lambda y: (y,fechaATimestamp(f),u),quitaNoAlfaOHash(t).split(' '))).filter(lambda x: (len(x[0])>1) and x[0][0]=='#')
rddPrimerasMenciones = rddHashtagsConUsuarioYFecha.reduceByKey(lambda x, y: x if x[0] < y[0] else y)
rddPuntuacionesUsuarios = rddPrimerasMenciones.join(rddAlcanceHashtags)
rddPuntuacionesUsuarios = rddPuntuacionesUsuarios.map(lambda (h, ((f, u), p)): (u, (p, [h]))).reduceByKey(
    lambda p, t: (p[0] + t[0], p[1] + t[1]))
print rddPuntuacionesUsuarios.takeOrdered(3, lambda x: -x[1][0])
