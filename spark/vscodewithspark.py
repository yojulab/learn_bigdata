from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext('local')
spark = SparkSession(sc)

textFile = spark.read.text("./README.md")       # warring ./ - or ../ 
cnt = textFile.count()
print(cnt)

contexts = textFile.take(4)
print(contexts)
linesWithSpark = textFile.filter(textFile.value.contains("Spark"))
print(linesWithSpark)
cnt = linesWithSpark.count()
print(cnt)

spark.stop()