# 导入模块
import findspark
from matplotlib import colors
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list

from operator import add

from wordcloud import WordCloud
import jieba
import matplotlib.pyplot as plt

# ======================== 初始化工作 =========================
# 指明SPARK_HOME
from cleaning_funs import filter_logs, map_logs, flat, map_remove_message_some, filter_message
from analysis_funs import map_remove_face_cq, map_jieba, filter_dict

findspark.init("/Users/zhinushannan/environment/spark-3.2.1-bin-hadoop3.2")
# jieba加载自定义字典
jieba.load_userdict("../jieba_dict/dict.txt")
# 新建spark应用
spark = SparkSession.builder.master("local").appName("app").getOrCreate()
sc = spark.sparkContext

# 读取文件
files = sc.textFile("../logs/2022-05-28.log")  # 评教最后一天
# files = sc.textFile("../logs/2022-05-29.log")  # 文理学院组织助农产品
# files = sc.textFile("../logs/2022-05-30.log")  # 猫死亡事件


# 数据清洗：将消息日志转为dataframe

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

logs_table = spark.createDataFrame(data=logs_list).rdd.map(map_remove_message_some).filter(filter_message).toDF()

# ============================== 词云统计 ======================================
# 筛选对应群聊、删除不必要的列
message_df = logs_table.rdd.filter(
    lambda message: message["group"] == "潘安湖二手物品转卖群(949682820)" or message["group"] == "新蜂生活超市(854452883)").toDF().drop(
    "time").drop("group").drop("user")
# 删除消息中的表情，使用jieba分词
message_df = message_df.rdd.map(map_remove_face_cq).map(map_jieba).toDF()

# 将全模式分词的结果合并成一个list
message_list = message_df.groupby("id").agg(collect_list('cut_all').alias('cut_all_list')).rdd.collect()
words = str(message_list[0]["cut_all_list"]).replace("[", "").replace("]", "").replace("'", "").split(", ")
# 统计词频
words_df = sc.parallelize(words).map(lambda x: (x, 1)).reduceByKey(add).toDF(schema=["word", "frequency"])
word_frequency = words_df.rdd.map(lambda word: (word["word"], word["frequency"])).filter(filter_dict).collect()
word_frequency_dict = {}
for i in word_frequency:
    word_frequency_dict[i[0]] = i[1]

color_list = ['#A60014', '#CC2438', '#A80A1D']  # 建立颜色数组
colormap = colors.ListedColormap(color_list)  # 调用

# 生成词云
wc = WordCloud(font_path="../fonts/FangZhengHeiTiJianTi-1.ttf",
               background_color='#FFFFFF', colormap=colormap, repeat=True,
               width=1600, height=800)

image = wc.fit_words(word_frequency_dict)

plt.imshow(image)
plt.axis("off")
plt.show()
