import pandas as pd
from itertools import combinations
from itertools import permutations 

class Phase1:

    def __init__(self, K2, K3, V2, V3):
        self.K2 = K2
        self.K3 = K3
        self.V2 = V2
        self.V3 = V3

    def map(self):
        data = pd.read_csv (r'./Data/testInput1.csv') #TODO read path from user
        self.K2 = pd.DataFrame(data, columns= ['Movie'])
        self.V2 = pd.DataFrame(data, columns= ['Userid','Rating'])

    def reduce(self):
        self.K3 = pd.DataFrame({'Userid': self.V2.Userid})
        self.V3 = pd.concat([self.K2.Movie, self.V2.Rating, self.K2.groupby(by='Movie')['Movie'].transform('count')],axis=1,keys=['Movie', 'Rating', 'NumOfRatings'])
        return(self.K3, self.V3)

class Phase2:

    def __init__(self, K3, V3):
        self.K3 = K3
        self.V3 = V3

    def map(self):
        horizontal_concat = pd.concat([self.K3, self.V3], axis=1)
        t3 = list(horizontal_concat.itertuples(index=False))
        return t3

    def sort(self, t3): #on a cluster this would send the data to diffent nodes before the reduce
        d3 = dict()
        i = 0
        while i < len(t3): #Go through the list and create a dictionary with all the values tied to the same user
            key = t3[i].Userid
            if key in d3: #if this user id is already in the dict add the values to this key
                d3[t3[i].Userid].append([t3[i].Movie,t3[i].Rating,t3[i].NumOfRatings])
            else: #Create a new key and give values to that key
                d3[t3[i].Userid] = [t3[i].Movie,t3[i].Rating,t3[i].NumOfRatings]
            i = i + 1
        return d3

    def reduce(self, d3):
        i = 0
        while i < len(d3):
            #loop through twice combine all the lists with append then remove duplicates later

        
        