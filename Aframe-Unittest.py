import unittest
import aframe as af
import pandas as pd
import numpy as np
import math
columns = ["alias", "employment", "friendIds","gender","id","name","nickname","userSince"]

class TestAframeFunctions(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestAframeFunctions, self).__init__(*args, **kwargs)
        self.set_attributes()
        
    def set_attributes(self):
        self.message_af = af.AFrame(dataverse='TinySocial', dataset='GleambookMessages')
        self.user_af = af.AFrame(dataverse='TinySocial', dataset='GleambookUsers')
        self.message_df = self.message_af.toPandas()
        self.user_df = self.user_af.toPandas()
        
    def test_head(self):
        for i in range(5):
            head = self.user_df.head()
            assert (head.shape[0] == 5)
            for col in ["alias", "employment", "friendIds", "id", "name", "userSince"]:
                assert (head.iloc[i][col] == self.user_df.iloc[i][col])
            for col in ["gender", "nickname"]:
                if (type(head.iloc[i][col])==float) and (type(self.user_df.iloc[i][col])==float): 
                    x = float(head.iloc[i][col])
                    y = float(self.user_df.iloc[i][col])
                    if (not math.isnan(x)) and (not math.isnan(y)):
                        assert (head.iloc[i][col] == self.user_df.iloc[i][col])
                else:
                    assert (head.iloc[i][col] == self.user_df.iloc[i][col])
                    
    def test_map(self):
        f = lambda x: len(str(x))
        names = self.user_df["name"]
        l_map = self.user_df['name'].map(f)
        count = l_map.shape[0]
        for i in range(count):
            assert (len(names[i]) == l_map[i])
    
##    def test_apply(self):
##        f = lambda x: len(str(x))
##        apply = self.user_df.apply(f)
##        for (i,col) in enumerate(columns):
##            count = sum([f(a) for a in self.user_df[col]])
##            #print(apply[i], count)
##            assert (apply[i] == count)
            
    def test_elementwiseMap(self):
        real_len = [len(i) for i in self.user_df['name']]
        name_len = self.user_af['name'].map('length')
        collect = name_len.collect()
        size = collect.shape[0]
        for row in range(size):
            a = collect[0][row]
            assert (a == real_len[row])
            
    def test_functionsWithArgumentsMap(self):
        name_contain = self.user_af['name'].map('contains', 'Suzan')
        collect = name_contain.collect()
        names = self.user_df['name']
        for (i,name) in enumerate(names):
            truth = 'Suzan' in name
            assert (truth == collect[0][i])
            
    def test_tablewiseApply(self):
        #message_af = af.AFrame(dataverse='TinySocial', dataset='GleambookMessages')
        fields = self.message_af.apply('get_object_fields')
        first_head = fields.head(1)
        real_fields = self.message_af.columns
        for i in first_head:
            for f in first_head[i]:
                assert (f['field-name'] == list(real_fields[i].keys())[0])
                if f['field-type'] == 'bigint':
                    assert (list(real_fields[i].values())[0] == 'int64')
                else:
                    assert (f['field-type'] == list(real_fields[i].values())[0])    
    
    def test_unnest(self):
        output = self.user_af.unnest(self.user_af['friendIds']).head()#user_af.unnest(user_af['friendIds'], appended=True, name='friendID').head()
        real = pd.DataFrame({'alias':self.user_df.alias.repeat(self.user_df.friendIds.str.len()),'friendID':np.concatenate(self.user_df.friendIds.values)})
        real_head = list(real['friendID'].loc[[4,5]].head())
        for (i, el) in enumerate(output[0]):
            assert(el == real_head[i])
            
    def test_add(self):
        real = self.user_af['id'].head()[0]
        output1 = self.user_af['id'].add(3).head()[0]
        output2 = (self.user_af['id']+3).head()[0]
        for i in range(5):
            assert(real[i]+3 == output1[i])
            assert(real[i]+3 == output2[i])
            
    def test_mul(self):
        real = self.user_af['id'].head()[0]
        output = self.user_af['id'].mul(3).head()[0]
        for i in range(5):
            assert(real[i]*3 == output[i])
            
    def test_toPandas(self):
        output_df = self.user_af.toPandas().head()
        real_df = self.user_df.head()
        output_columns = list(self.user_af.toPandas().columns)
        real_columns = list(self.user_df.columns)
        testing_columns = ['alias', 'gender', 'id', 'name', 'nickname']
        for i in range(len(real_columns)):
            assert(output_columns[i] == real_columns[i])
        for col in testing_columns:
            for a in range(5):
                out = output_df[col][a]
                real = real_df[col][a]
                if type(out) == float or type(real) == float:
                    if math.isnan(out):
                        out = 0.0
                    if math.isnan(real):
                        real = 0.0
                assert(out == real)
                
    def test_withColumn(self):
        output = self.user_af.withColumn('id_3', self.user_af['id']+3).head()['id_3']
        real = self.user_df.sort_values(by=['id']).head()['id']
        for (i,el) in enumerate(list(output)):
            assert(output.iloc[i] == real.iloc[i]+3)
            
    def test_persisting(self):
        #message_af = af.AFrame(dataverse='TinySocial', dataset='GleambookMessages')
        #user_af = af.AFrame(dataverse='TinySocial', dataset='GleambookUsers')
        self.test_toPandas()
        self.test_withColumn()
    
if __name__ == "__main__":
    unittest.main()
