from aframe import AFrame
import pandas as pd

if __name__ == '__main__':

    tmp = AFrame(dataverse='test', dataset='TweetItems')
    # print(tmp)
    # print(tmp['text'])
    text = tmp['truncated'] == True
    print(text.head(2))
    print(text.query)
    print(type(text.head(2)))
    text = tmp[tmp['truncated'] == True]
    print(text.head(2))
    text = (tmp['truncated'] == True) & (tmp['favorited'] == False)
    print(text.head(2))
    text = tmp[(tmp['truncated'] == True) & (tmp['favorited'] == False)]
    print(text.head(2))

    sl = tmp[0:2]
    print(sl.query)
    pd.DataFrame().groupby()

    # text = tmp[(tmp['truncated'] == True) & (tmp['favorited'] == False)]
    # print(text.head(2))
    # trunc = tmp['truncated'].head(5)
    # # print(trunc)
    # trucTrue = tmp[(tmp['favorited'] == False) & (tmp['retweet_count'] == 0) & (tmp['truncated'] == False)]
    #
    # print(trucTrue.query)
    # trucTrue.get_dataverse()
    # aTruc = trucTrue.toAframe()
    # print(aTruc)

    # print(trucTrue.head())
    # print(trucTrue.collect())
    # print(len(tmp))

    # print(trucTrue.head())
    # df = tmp.toPandas(4)
    # df2 =(df['truncated'] == True) & (df['favorited'] == False)
    # print(df2)
    # print(df)
    # a = df[(df['truncated'] == True) & (df['retweet_count'] == 0)]
    # print(a)
    # print(df[['truncated', 'favorited', 'retweet_count']])
    #
    # trc = df['truncated'] == True
    # print(type(trc))
    # print(df[df['truncated'] == True])
    # q1 = tmp['retweet_count'] == 0
    #
    # print(q1.head())
