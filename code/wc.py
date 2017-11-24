import unicodedata
# u2a converts Unicode to ASCII
def u2a(u): return str(unicodedata.normalize('NFKD',u).encode('ascii','ignore'))

# strip removes any non-alpha characters
def strip(s): return ''.join(filter(str.isalpha, s))

import findspark
findspark.init()

import pyspark
sc = pyspark.SparkContext.getOrCreate()
books = sc.textFile("file:///home/big/wordcount/*.txt")

split = books.flatMap(lambda line: line.split())
asc = split.map(u2a)
stripped = asc.map(strip)
notempty = stripped.filter(lambda w: len(w)>0)

lower = notempty.map(lambda w: w.lower())
mapped = lower.map(lambda w: (w,1))
wordcount = mapped.reduceByKey(lambda x,y: x+y)

reorder = wordcount.map(lambda (k,v):(v,k))
sort = reorder.sortByKey(False)

for k,v in sort.collect():
    print k,v
