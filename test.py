# encoding=utf-8
import jieba

jieba.enable_paddle()

seg_list = jieba.cut("我来到北京清华大学，但是。我来自北京大学", cut_all=True, use_paddle=True)
print(",".join(seg_list))

