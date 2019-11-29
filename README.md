# dash-fink
Dash based interface to Livy/HBase/Fink

To start developing
> conda deactivate
> d:\workspace\Script\dash-env\Script\activate.bat

> python dash-asynchronous.py

https://delta.io/
https://docs.delta.io/latest/index.html

hdfs dfs -du -h /lsst/DC2/cosmoDC2/xyz_v1.1.4_hive

spark-submit --master mesos://vm-75063.lal.in2p3.fr:5050
web ui -> https://wuip.lal.in2p3.fr:24444/

###pyspark --packages io.delta:delta-core_2.11:0.4.0,org.influxdb:influxdb-java:2.14 --master spark://134.158.75.222:7077  --driver-memory 29g --total-executor-cores 85 --executor-cores 17 --executor-memory 29g
pyspark --packages io.delta:delta-core_2.11:0.4.0,org.influxdb:influxdb-java:2.14 --master yarn  --driver-memory 29g --total-executor-cores 85 --executor-cores 17 --executor-memory 29g

pyspark --packages io.delta:delta-core_2.11:0.4.0,org.influxdb:influxdb-java:2.14 --master mesos://vm-75063.lal.in2p3.fr:5050  --driver-memory 29g --total-executor-cores 85 --executor-cores 17 --executor-memory 29g


export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Xmx5g"

org.influxdb:influxdb-java:2.14

/lsst/DC2/cosmoDC2/cosmoDC2_v1.1.4_image.parquet
df = spark.read.format("parquet").load("/user/julien.peloton/xyz_v1.1.4.parquet")
df.show()

df.write.format("delta").save("/user/chris.arnault/xyz")

source1 = "/lsst/DC2/cosmoDC2/cosmoDC2_v1.1.4_image.parquet"
source2 = "/user/julien.peloton/xyz_v1.1.4.parquet"
dest = "/user/chris.arnault/xyz"

df = spark.read.format("parquet").load(source1)
df.show()
df.count()/(1024*1024)

### df55 = df.where("ra > 55 and ra < 55.0001")
df55.write.format("delta").save(dest)

df = spark.read.format("delta").load(dest)
df.show()


Scala:

val df = spark.read.format("parquet").load("/user/julien.peloton/xyz_v1.1.4.parquet")
df.show
df.write.format("delta").save("/user/chris.arnault/xyz")
val df2 = spark.read.format("delta").load("/user/chris.arnault/xyz")
df.show

val df = spark.read.format("parquet").load("/lsst/DC2/cosmoDC2/xyz_v1.1.4_hive")
df.show
df.write.format("delta").save("/user/chris.arnault/xyz2")
val df2 = spark.read.format("delta").load("/user/chris.arnault/xyz2")
df.show

/lsst/DC2/cosmoDC2/cosmoDC2_v1.1.4_image.parquet
val df = spark.read.format("parquet").load("/lsst/DC2/cosmoDC2/cosmoDC2_v1.1.4_image.parquet")
df.write.format("delta").save("/user/chris.arnault/xyz3")

supervision.lal.in2p3.fr/
http://vm-75222.lal.in2p3.fr/


rows = 100000
offset = 40.0
field = 40.0
dest = "/user/chris.arnault/xyz"
values = [('ra', offset + random.random()*field) for i in range(rows)]
df = spark.createDataFrame(values, ['ra',])
df.write.format("delta").mode("overwrite").save(dest)

for j in range(1000):
    values = [('ra', offset + random.random()*field) for i in range(rows)]
    df = spark.createDataFrame(values, ['ra',])
    df.write.format("delta").mode("append").save(dest)
    print("j = {}".format(j))

df = spark.read.format("delta").load(dest)
df.count()
df.show()

df = spark.read.format("delta").option("versionAsOf", 2).load(dest)

#### History management
from delta.tables import *
table = DeltaTable.forPath(spark, dest)
h = table.history()
h.show()
h.filter("version < 10").show()

#### repartition
df.write.format("delta").partitionBy("ra").option("overwriteSchema", "true").mode("overwrite").save(dest)


import random
from pyspark.sql.functions import *
from pyspark.sql.types import *

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

values = [(ra_value(), dec_value(), z_value()) for i in range(100)]
df = spark.createDataFrame(values, ['ra','dec', 'z'])
df.show()

flux_field = 10.0

df = df.withColumn('flux', flux_field * rand())

df.withColumn('SN', when(df.flux > 8, True).otherwise(False)).show()
