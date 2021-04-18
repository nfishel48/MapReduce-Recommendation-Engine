import pandas as pd

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
        self.V3 = pd.concat([self.K2.Movie, self.V2.Rating, self.K2.groupby(by='Movie')['Movie'].transform('count')],axis=1,keys=['Movie', 'Rating', '#Ratings'])
        return(self.K3, self.V3)
        