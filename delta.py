
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import sys
import random

rows = 100

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Delta").getOrCreate()

    for i, arg in enumerate(sys.argv[1:]):
        a = arg.split("=")
        print(i, arg, a)
        if a[0] == "rows":
            rows = int(a[1])

    ra_offset = 40.0
    ra_field = 40.0
    dec_offset = 20.0
    dec_field = 40.0
    z_offset = 0.0
    z_field = 5.0

    def ra_value():
      return ra_offset + random.random()*ra_field

    def dec_value():
      return dec_offset + random.random()*dec_field

    def z_value():
      return z_offset + random.random()*z_field

    values = [(ra_value(), dec_value(), z_value()) for i in range(rows)]
    df = spark.createDataFrame(values, ['ra','dec', 'z'])
    df.show()

    flux_field = 10.0

    df = df.withColumn('flux', flux_field * rand())

    df.withColumn('SN', when(df.flux > 8, True).otherwise(False)).show()

    flux_field = 10.0

    df = df.withColumn('flux', flux_field * rand())

    df.withColumn('SN', when(df.flux > 8, True).otherwise(False)).show()

    spark.stop()
