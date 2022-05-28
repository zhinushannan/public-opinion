import jieba
import jieba.posseg as pseg

jieba.enable_paddle()
jieba.load_userdict("./jieba_dict/dict.txt")

words = pseg.cut("老板，我想要一瓶花露水。.", use_paddle=True)
for word, flag in words:
    print(word, flag)
