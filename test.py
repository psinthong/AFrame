from aframe import AFrame
import pandas as pd

if __name__ == '__main__':

    tmp = AFrame(dataverse='testSK', dataset='TweetItems')
    # print(tmp)
    # print(tmp['text'])
    # text = tmp['source'].head(2)
    # print(text)
    # trunc = tmp['truncated'].head(5)
    # # print(trunc)
    trucTrue = tmp[(tmp['favorited'] == False) & (tmp['retweet_count'] == 0) & (tmp['truncated'] == False)]
    #
    # print(trucTrue.query)
    # trucTrue.get_dataverse()
    # aTruc = trucTrue.toAframe()
    # print(aTruc)

    # print(trucTrue.head())
    # print(trucTrue.collect())
    # print(len(tmp))

    # print(trucTrue.head())
    df = tmp.toPandas(4)
    # print(df)
    # a = df[(df['truncated'] == True) & (df['retweet_count'] == 0)]
    # print(a)

    print(df['retweet_count'] == 0)
    print(df[df['retweet_count'] == 0])
    q1 = tmp['retweet_count'] == 0

    print(q1.head())
