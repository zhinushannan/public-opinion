import re
import jieba
from pyspark.sql.types import Row


def map_remove_face_cq(line):
    message = str(line["_c3"])
    result = re.compile(r"(\[CQ:face,id=(\d+)])").findall(str(message))
    for i in result:
        message.replace(i[0], "")
    return message


jieba.enable_paddle()
jieba.load_userdict("../jieba_dict/dict.txt")


def map_jieba(line):
    jieba_result = jieba.cut(line)
    result = []
    for i in jieba_result:
        result.append(i)
    jieba_result = jieba.cut(line, cut_all=True)
    all = []
    for i in jieba_result:
        all.append(i)
    return Row(
        raw=line,
        cut_hmm=str(result)[1:-1],
        cut_all=str(all)[1:-1]
    )
