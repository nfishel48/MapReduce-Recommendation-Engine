import pandas as pd
import Modules.Module1 as Phases


class Runner:

    def __main__(self):
        K2 = pd.DataFrame()
        K3 = pd.DataFrame()
        V2 = pd.DataFrame()
        V3 = pd.DataFrame()
        Phase1 = Phases.Phase1(K2,K3,V2,V3) #create a instance of Phase1 then run the map and reduce functions
        Phase1.map()
        tuple1 = Phase1.reduce()
        K3 = tuple1[0]
        V3 = tuple1[1]
        ## Debug print satements
        horizontal_concat = pd.concat([K3, V3], axis=1)
        print("\n Results of Phase1 \n")
        print(horizontal_concat)
        #####
        Phase2 = Phases.Phase2(K3, V3)
        t3 = Phase2.map()
        d3 = Phase2.sort(t3)
        Phase2.reduce(d3)


Run = Runner()
Run.__main__()