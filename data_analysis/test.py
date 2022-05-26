from __future__ import print_function, division

from pyspark.sql import SparkSession

from data_analysis.segmentation_funs import map_remove_face_cq

"""  新建spark应用  """
spark = SparkSession.builder.master("local").appName("app").getOrCreate()

message_df = spark.read.csv("../result/*.csv").drop("_c0").drop("_c1").drop("_c2")
message_df = message_df.filter(message_df["_c3"] != "null").rdd.map(map_remove_face_cq).collect()

for i in message_df:
    print(i)

