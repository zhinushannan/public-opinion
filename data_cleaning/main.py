from __future__ import print_function, division

import re
import sys

from pyspark.sql import SparkSession

from data_cleaning import map_logs, flat


def filter_logs(line):
    match_upload = re.search(r"(^\[\S+ \S+]) \[INFO]: 群 (\S+) 内 ([\s\S]+) 上传了文件: ([\s\S]+)", line)
    match_member_add = re.search(r"(^\[\S+ \S+]) \[INFO]: 新成员 (\S+) 进入了群 ([\s\S]+)", line)
    match_member_leave = re.search(r"(^\[\S+ \S+]) \[INFO]: 成员 (\S+) 离开了群 ([\s\S]+)", line)

    if match_upload is None and match_member_add is None and match_member_leave is None:
        return True
    else:
        return False


"""  新建spark应用  """
spark = SparkSession.builder.master("local").appName("app").getOrCreate()
sc = spark.sparkContext

"""  读取文件并建立dataframe  """
files = sc.textFile("./2022-05-23.log")

logs = files.map(map_logs)

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

logs_table = spark.createDataFrame(data=logs_list, schema=["time", "group", "user", "message"]).drop("_5")
logs_table = logs_table.filter(logs_table['user'] != 'self')
