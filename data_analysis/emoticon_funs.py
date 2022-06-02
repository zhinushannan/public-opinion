import re

from pyspark.sql.types import Row


def map_build_face_df(line):
    message = str(line)
    result = re.compile(r"(\[CQ:face,id=(\d+)])").findall(str(message))
    face = ""
    for i in result:
        face += i[1] + ", "
    face = face[0:-2]
    return Row(
        id=1,
        face=face
    )


def map_emotion(line):
    pass
