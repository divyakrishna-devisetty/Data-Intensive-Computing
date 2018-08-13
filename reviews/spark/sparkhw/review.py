import json
from pyspark import SparkConf, SparkContext

review_input = "/home/dk/Desktop/pythonhw/hw3/review.data"
meta_input = "/home/dk/Desktop/pythonhw/hw3/meta.data"
review_output="/home/dk/Desktop/pythonhw/hw3/output1"
meta_output="/home/dk/Desktop/pythonhw/hw3/output2"

conf = SparkConf()
sc=SparkContext(conf=conf)

review_data = sc.textFile(review_input)
count = review_data.filter(lambda line: len(json.loads(line)['reviewText'].split()) > 100)
mapRdd = count.map(lambda line: (json.loads(line)['overall'], (json.loads(line)['asin'], json.loads(line)['reviewText'])))
outputRdd = mapRdd.sortByKey(ascending=False)
outputRdd.saveAsTextFile(review_output)

print "Number of reviews with more than 100 words are:{0}".format(count.count())

metaData = sc.textFile(meta_input)
count_review = metaData.filter(lambda line: json.loads(line)['categories']== "Music")
count_review = count_review.map(lambda line: (json.loads(line)['asin'],json.loads(line)['categories']))

review= review_data.map(lambda line:(json.loads(line)['asin'],len(json.loads(line)['reviewText'].split())))

total=review.join(count_review)

total.saveAsTextFile(meta_output)

newRdd = total.map(lambda line:line[1][0])

avg = newRdd.sum()/total.count()

print "Average Number words in each review of reviews in music category is {0}".format(avg)
