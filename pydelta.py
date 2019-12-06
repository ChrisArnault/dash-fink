
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


import sys
import os
import random

rows = 1000
dest = "/user/chris.arnault/xyz"
batch_size = 1000

import time

class Stepper(object):
    previous_time = None

    def __init__(self):
        self.previous_time = time.time()

    def show_step(self, label='Initial time'):
        now = time.time()
        delta = now - self.previous_time

        if delta < 60:
            t = '0h0m{:.3f}s'.format(delta)
        elif delta < 3600:
            m = int(delta / 60)
            s = delta - (m*60)
            t = '0h{}m{:.3f}s'.format(m, s)
        else:
            h = int(delta / 3600)
            d = delta - (h*3600)
            m = int(d / 60)
            s = d - (m*60)
            t = '{}h{}h{:.3f}s'.format(h, m, s)

        print('--------------------------------', label, t)

        self.previous_time = now

        return delta

if __name__ == "__main__":
    s = Stepper()
    spark = SparkSession.builder.appName("Delta").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    s.show_step("init")

    for i, arg in enumerate(sys.argv[1:]):
        a = arg.split("=")
        print(i, arg, a)
        if a[0] == "rows":
            rows = int(a[1])
        if a[0] == "batch_size":
            batch_size = int(a[1])


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

    os.system("hdfs dfs -rm -r -f {}".format(dest))

    print("============= create the DF with ra|dec|z")

    factor = 1000000

    print("factor={}".format(factor))
    print("rows={}".format(rows))
    print("batch_size={}".format(batch_size))

    batch_size *= factor
    rows *= factor

    batches = int(rows/batch_size)
    for batch in range(batches):
        print("batch #{}".format(batch))
        values = [(ra_value(), dec_value(), z_value()) for i in range(batch_size)]
        df = spark.createDataFrame(values, ['ra','dec', 'z'])
        if batch == 0:
            # df.coalesce(10000)
            ### df.write.format("delta").partitionBy("ra").save(dest)
            df.write.format("parquet").save(dest)
        else:
            # df.coalesce(10000)
            ### df.write.format("delta").partitionBy("ra").mode("append").save(dest)
            df.write.format("parquet").mode("append").save(dest)

    flux_field = 10.0

    print("============= add the column for flux")
    df = df.withColumn('flux', flux_field * rand())
    df.write.format("parquet").mode("append").save(dest)


    # df.write.format("delta").partitionBy("ra").mode("overwrite").option("mergeSchema", "true").save(dest)
    # df.show()

    spark.stop()
    exit()


    names = "azertyuiopqsdfghjklmwxcvbn1234567890"
    print("============= add {} columns".format(len(names)))
    for c in names:
        df = df.withColumn(c, rand())

    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(dest)

    print("============= add a column for SN tags")
    df.withColumn('SN', when(df.flux > 8, True).otherwise(False))

    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(dest)

    os.system("hdfs dfs -du -h {}".format(dest))

    df = spark.read.format("delta").load(dest)
    df.show()
    print("count = {} partitions={}".format(df.count(), df.rdd.getNumPartitions()))


    spark.stop()
