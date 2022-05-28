from __future__ import print_function, division

from operator import add

import numpy as np
from PIL import Image
from matplotlib import pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list
from wordcloud import WordCloud, ImageColorGenerator

from data_analysis.segmentation_funs import map_remove_face_cq, map_jieba

"""  新建spark应用  """
spark = SparkSession.builder.master("local").appName("app").getOrCreate()
sc = spark.sparkContext

message_df = spark.read.csv("../result/*.csv")
message_df = message_df.rdd.filter(
    lambda message: message["_c1"] == "潘安湖二手物品转卖群(949682820)" or message["_c1"] == "新蜂生活超市(854452883)").toDF().drop(
    "_c0").drop("_c1").drop("_c2")
message_df = message_df.rdd.map(map_remove_face_cq).map(map_jieba)

message_list = message_df.collect()

a = message_df.toDF().groupby("id").agg(collect_list('cut_all').alias('cut_all_list')).rdd.collect()

words = str(a[0]["cut_all_list"]).replace("[", "").replace("]", "").replace("'", "").split(", ")

words_df = sc.parallelize(words).map(lambda x: (x, 1)).reduceByKey(add).toDF(schema=["word", "frequency"])
# words_df.orderBy(-col("frequency")).show(100)
# words_df.write.mode("append").csv("./jieba_result") # 生成文件

word_frequency = words_df.rdd.map(lambda word: (word["word"], word["frequency"])).collect()

word_frequency_dict = {}
for i in word_frequency:
    word_frequency_dict[i[0]] = i[1]


wc = WordCloud(font_path="../tools/msjhl.ttc",
               background_color='#FFFFFF', colormap="Reds", repeat=True)

wc.fit_words(word_frequency_dict)

plt.imshow(wc)
plt.show()
