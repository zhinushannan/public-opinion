from __future__ import print_function, division

from pyspark.sql import SparkSession

from data_analysis.segmentation_funs import map_remove_face_cq, map_jieba

"""  新建spark应用  """
spark = SparkSession.builder.master("local").appName("app").getOrCreate()

message_df = spark.read.csv("../result/*.csv")
message_df = message_df.rdd.filter(
    lambda message: message["_c1"] == "潘安湖二手物品转卖群(949682820)" or message["_c1"] == "新蜂生活超市(854452883)").toDF().drop(
    "_c0").drop("_c1").drop("_c2")
message_df = message_df.rdd.map(map_remove_face_cq).map(map_jieba)

message_df.toDF().write.mode("append").csv("./jieba_result.csv")
