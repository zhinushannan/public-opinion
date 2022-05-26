import re

from pyspark.sql.types import Row


def filter_logs(line):
    """
    过滤掉“上传文件”、“成员加入”、“成员离开”三类事件
    :param line:
    :return:
    """
    match_upload = re.search(r"(^\[\S+ \S+]) \[INFO]: 群 (\S+) 内 ([\s\S]+) 上传了文件: ([\s\S]+)", line)
    match_member_add = re.search(r"(^\[\S+ \S+]) \[INFO]: 新成员 (\S+) 进入了群 ([\s\S]+)", line)
    match_member_leave = re.search(r"(^\[\S+ \S+]) \[INFO]: 成员 (\S+) 离开了群 ([\s\S]+)", line)

    if match_upload is None and match_member_add is None and match_member_leave is None:
        return True
    else:
        return False


def map_logs(line):
    """
    将过滤后的日志的发消息和收消息的事件构成新的RDD
    :param line:
    :return:
    """
    match_send = re.search(r"(^\[\S+ \S+]) \[INFO]: 发送群 (\S+) 的消息: ([\s\S]+)", line)
    match_get = re.search(r"(^\[\S+ \S+]) \[INFO]: 收到群 (\S+) 内 ([\s\S]+) 的消息: ([\s\S]+)", line)

    if match_send is None and match_get is None:
        return Row(
            time=None,
            group=None,
            user=None,
            message=line,
        )

    if match_send is not None:
        return Row(
            time=match_send.group(1),
            group=match_send.group(2),
            user="self",
            message=match_send.group(3),
        )
    if match_get is not None:
        return Row(
            time=match_get.group(1),
            group=match_get.group(2),
            user=match_get.group(3),
            message=match_get.group(4),
        )


def flat(l):
    """
    构建新自增列
    :param l:
    :return:
    """
    for k in l:
        if not isinstance(k, (list, tuple)):
            yield k
        else:
            yield from flat(k)


def map_remove_message_some(line):
    """
    移除message中不必要的信息
    1、 message_id
    2、 回复事件
    3、 @ 事件
    :param line:
    :return:
    """
    message = str(line["_4"])
    # 移除 message_id
    message = message[0:message.rfind("(")]
    # 移除 回复 事件
    result = re.compile(r"(\[CQ:reply,id=\d+])+").findall(line)
    for i in result:
        print(i)
        line = line.replace(i, "")
    # 移除 @ 事件
    result = re.compile(r"(\[CQ:at,qq=\w+])+").findall(line)
    for i in result:
        print(i)
        line = line.replace(i, "")

    return Row(
        time=line["_1"],
        group=line["_2"],
        user=line["_3"],
        message=message
    )


