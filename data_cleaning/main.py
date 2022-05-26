from __future__ import print_function, division

from pyspark.sql import SparkSession

from data_cleaning.funs import filter_logs, map_logs, flat, map_remove_message_some

"""  新建spark应用  """
spark = SparkSession.builder.master("local").appName("app").getOrCreate()
sc = spark.sparkContext

"""  读取文件并建立dataframe  """
files = sc.textFile("./2022-05-23.log")

logs = files.filter(filter_logs).map(map_logs)

logs_df = logs.toDF()

"""  合并换行的消息  """
logs_rdd = logs_df.rdd.zipWithIndex()
logs_rdd = logs_rdd.map(lambda x: list(flat(x)))
i = 0
logs_list = logs_rdd.collect()
while i < len(logs_list):
    if i + 1 < len(logs_list) and logs_list[i + 1][0] is None:
        logs_list[i][3] += logs_list[i + 1][3]
        logs_list.pop(i + 1)
    else:
        i += 1

logs_table = spark.createDataFrame(data=logs_list).rdd.map(map_remove_message_some).toDF()
logs_table = logs_table.filter(logs_table["user"] != "self")

