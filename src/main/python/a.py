# import pandas as pd
#
# dataScore = pd.read_csv(r'F:\com\data\TotScore.csv')
# # dataScore['jsy'] = dataScore['jsy'].map(lambda x: int(x)).astype(str)
# dataScore['jsy'] = dataScore['jsy'].astype(str)
# f = open(r'F:\com\data\accidentData.csv',encoding="utf-8")
# accident = pd.read_csv(f, low_memory=False)
# f.close()
# accident['工号'].fillna('000000',inplace=True)
# accident['工号'] = accident['工号'].map(lambda x: int(x)).astype(str)
# accident = accident[['事故编号','工号','责任车牌号','事故原因','事故等级','发生时间']]
# print(accident)
# print(dataScore)
# res = pd.merge(dataScore,accident,left_on='jsy',right_on='工号')
# print(res)
# res.to_csv(r'F:\com\data\accidentScore.csv',index=None,encoding='utf_8_sig')
#
# def calBins(series,bins):
#     cutBins = pd.cut(series, bins,right=False)
#     v = pd.value_counts(cutBins).to_frame()
#     v.reset_index(inplace=True)
#     v.columns = ['bins', 'scoreCount']
#     v['bins'] = v['bins'].map(lambda x: str(str(x).replace('(', '').replace(')','').replace('[','').replace(']', '').replace(', ', '-')))
#     v = v.sort_values(by="bins", ascending=True)
#     return v
#
# bins1 = [0,10,20,30,40,50,60,70,80,90,100]
# v1 = calBins(dataScore['comprehensiveScore'], bins1)
# print(v1)
# v2 = calBins(res['comprehensiveScore'], bins1)
# print(v2)
# v2.rename(columns={'scoreCount': 'accidentCount'}, inplace=True)
# accidentRate = v1.merge(v2, on='bins')
# accidentRate['accidentRate'] = accidentRate['accidentCount']/accidentRate['scoreCount']
# accidentRate['accidentRate'].fillna(0,inplace=True)
# accidentRate.to_csv(r'F:\com\data\accidentRate.csv',index=None,encoding='utf_8_sig')
#
