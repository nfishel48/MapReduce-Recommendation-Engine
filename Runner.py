import pandas as pd
import Modules.Module1 as StepOne


class Runner:

    def __main__(self):
        K2 = pd.DataFrame()
        K3 = pd.DataFrame()
        V2 = pd.DataFrame()
        V3 = pd.DataFrame()
        Phase1 = StepOne.Phase1(K2,K3,V2,V3) #create a instance of Phase1 then run the map and reduce functions
        Phase1.map()
        tuple1 = Phase1.reduce()
        K3 = tuple1[0]
        V3 = tuple1[1]
        ## Debug print satements
        horizontal_concat = pd.concat([K3, V3], axis=1)
        print("\n Results of Phase1 \n")
        print(horizontal_concat)
        #####

Run = Runner()
Run.__main__()