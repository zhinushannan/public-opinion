with open("./jieba_dict/filter_dict.txt", "r") as fp:
    dict_list = set(fp.read().split("\n"))

with open("./jieba_dict/filter_dict.txt", "w") as fp:
    fp.write("\n".join(dict_list))
