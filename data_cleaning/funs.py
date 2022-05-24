import re

from pyspark.sql.types import Row


def map_logs(line):
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
    for k in l:
        if not isinstance(k, (list, tuple)):
            yield k
        else:
            yield from flat(k)
