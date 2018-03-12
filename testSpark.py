import pandas as pd
import findspark
findspark.init()
import pyspark
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.functions import *


if __name__ == '__main__':
    sc = SparkContext(appName="app")
    sqlContext = SQLContext(sc)
    data = [('Amy', 25), ('Joe', 30), ('John', 22), ('Jane', 20), ('Tim', 26)]
    rdd = sc.parallelize(data)
    pySparkDF = sqlContext.createDataFrame(rdd, ['name', 'age'])
    pdf = pySparkDF[(pySparkDF['age'] >= 22) & (pySparkDF['name'] != 'John')]
    pdf.show()