import pandas as pd
import numpy as np
from scipy.stats import stats
from itertools import combinations
from itertools import permutations 

class Phase1:

    def __init__(self, K2, K3, V2, V3):
        self.K2 = K2
        self.K3 = K3
        self.V2 = V2
        self.V3 = V3

    def map(self):
        data = pd.read_csv (r'./Data/testInput2.csv') #TODO read path from user
        print("Start Phase1")
        self.K2 = pd.DataFrame(data, columns= ['Movie'])
        self.V2 = pd.DataFrame(data, columns= ['Userid','Rating'])

    def reduce(self):
        self.K3 = pd.DataFrame({'Userid': self.V2.Userid})
        self.V3 = pd.concat([self.K2.Movie, self.V2.Rating, self.K2.groupby(by='Movie')['Movie'].transform('count')],axis=1,keys=['Movie', 'Rating', 'NumOfRatings'])
        print("Phase1 finished")
        return(self.K3, self.V3)

class Phase2:

    def __init__(self, K3, V3):
        self.K3 = K3
        self.V3 = V3

    def map(self):
        horizontal_concat = pd.concat([self.K3, self.V3], axis=1)
        print("Starting Phase2")
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
                d3[t3[i].Userid] = []
                d3[t3[i].Userid].append([t3[i].Movie,t3[i].Rating,t3[i].NumOfRatings])
            i = i + 1
        return d3

    def reduce(self, d3):
        i = 0
        while i < len(d3): #Generate all unique combos of movies
            j = 0
            while j < len(d3[i])-1:
                d3[i][j][len(d3[i][j]):]=d3[i][j+1]
                if j+1 == len(d3[i])-1:
                    d3[i][j+1][len(d3[i][j+1]):]=d3[i][0]
                    del d3[i][j+1][6:8]
                j = j + 1
            i = i + 1
        
        #Calcuate stats for phase3
        i = 0
        while i < len(d3):
            j = 0
            print(f"Reducing...{i}/{len(d3)}")
            while j < len(d3[i]):
                ratingProduct = d3[i][j][1] * d3[i][j][4]
                d3[i][j].append(ratingProduct)
                rating1Squared = d3[i][j][1] * d3[i][j][1]
                d3[i][j].append(rating1Squared)
                rating2Squared = d3[i][j][4] * d3[i][j][4]
                d3[i][j].append(rating2Squared)
                j = j +1
            i = i +1
        print("Phase2 finished")
        return d3

class Phase3:

    def map(self, d3):
        i = 0
        K4 = pd.DataFrame([], columns=['Movie1', 'Movie2'])
        V4 = pd.DataFrame([], columns=['rating1', 'numOfRating1', 'rating2', 'numOfRating2', 'ratingProduct', 'rating1Squared', 'rating2Squared'])
        print("Starting Phase3")
        while i < len(d3):
            print(f"Mapping...{i}/{len(d3)}")
            j = 0
            while j < len(d3[i]):
                newrow = {'Movie1': d3[i][j][0], 'Movie2': d3[i][j][3]}
                K4 = K4.append(newrow, ignore_index=True)
                newrow2 = {'rating1':d3[i][j][1], 'numOfRating1':d3[i][j][2], 'rating2':d3[i][j][4], 'numOfRating2':d3[i][j][5], 'ratingProduct':d3[i][j][6], 'rating1Squared':d3[i][j][7], 'rating2Squared':d3[i][j][8]}
                V4 = V4.append(newrow2, ignore_index=True)
                j = j + 1
            i = i + 1
        final = pd.concat([K4, V4], axis=1)
        return(final)

        
    def reduce(self, final):
        results = pd.DataFrame([], columns=['Movie1', 'Movie2', 'Similarity']) 
        i = 0
        while i < len(final):
            print(f"Reducing...{i}/{len(final)}")
            similarity = final.loc[i,"ratingProduct"] / final.loc[i, "numOfRating1"] * final.loc[i, "numOfRating2"] 
            newrow = {'Movie1':final.loc[i, "Movie1"], 'Movie2':final.loc[i, "Movie2"], 'Similarity':similarity}
            results = results.append(newrow, ignore_index=True)
            i = i + 1
        
        results.to_csv(r'./out.csv', header=None, index=None, sep=' ', mode='w')
        print(results)