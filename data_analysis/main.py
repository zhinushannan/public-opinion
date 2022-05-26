from __future__ import print_function, division

from operator import add

from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list

from data_analysis.emoticon_funs import map_build_face_df

"""  新建spark应用  """
spark = SparkSession.builder.master("local").appName("app").getOrCreate()
sc = spark.sparkContext

message_df = spark.read.csv("../result/*.csv").drop("_c0").drop("_c1").drop("_c2")
message_df = message_df.filter(message_df["_c3"] != "null")

""" face 分析 """
# 将所有语句中的face_id提取出来，并组成一个标准的list
face_ids = message_df.rdd.filter(lambda message: "[CQ:face" in str(message)).map(map_build_face_df).toDF().groupby(
    "id").agg(collect_list('face').alias('face_list')).drop("id").rdd.collect()[0]["face_list"]

face_ids = str(face_ids).replace("[", "").replace("]", "").replace("'", "").split(", ")

# 统计表情包出现的频率
sc.parallelize(face_ids).map(lambda x: (x, 1)).reduceByKey(add).toDF(schema=["face_id", "frequency"]).show()

