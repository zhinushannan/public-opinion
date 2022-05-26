import re

from pyspark.sql.types import Row


def map_remove_face_cq(line):
    message = str(line["_c3"])
    result = re.compile(r"(\[CQ:face,id=(\d+)])").findall(str(message))
    for i in result:
        message.replace(i[0], "")
    return message
