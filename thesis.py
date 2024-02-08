from neo4j import GraphDatabase
import pandas as pd
import numpy as np
from time import time as timer 


class experiments: 
    def __init__(self):

        self.databases = ["grundriss2", "grundriss2RDF", "grundriss6", "grundriss6RDF", "grundriss8", "grundriss8RDF", "grundriss10", "grundriss10RDF"]
        
        # For Connection to the Neo4j Database Management Sytsem
        self.__URI = "bolt://localhost:7687"
        self.__AUTH = ("neo4j", "kais1234")

        # Adding the execution times for graph (LPG and RDF graph)
        self.graphs_measures={}
    
        # Number for running each query again -> to get a stable result
        self.numberOfIteration = None
    

    def experiment(self, file, numberOfIteration):
    
        self.numberOfIteration = numberOfIteration

        with  GraphDatabase.driver(self.__URI, auth=self.__AUTH) as driver:
            for i in range(len(self.databases)):
                if "RDF" in self.databases[i]: 
                    dbType = "RDF"
                else:
                    dbType = "LPG"

                with driver.session(database=self.databases[i]) as session:
                    self.queries =self.getQueriesFromFile(file)

                    # Execute first experiment
                    if "1" in file:
                        session.execute_read(
                            self.setup, 
                            dbType, 
                            self.databases[i]
                        )
                
                    # Execute second experiment
                    if "2" in file:
                         session.execute_write(
                            self.setup, 
                            dbType, 
                            self.databases[i]
                        )
                        
       
    # Extract queries according their types and the run the queries against the graphs by running method 
    def setup(self, tx, dbType, nameDB):
       
       # Query of type 1
       measure_type1 = self.executeQueries(tx,self.queries[self.queries['Type']==1][dbType].to_list(), 1)
       # Query of type 2
       measure_type2 = self.executeQueries(tx, self.queries[self.queries['Type']==2][dbType].to_list(), 2)
       # Query of type 3
       measure_type3 = self.executeQueries(tx, self.queries[self.queries['Type']==3][dbType].to_list(), 3)

       # Safe results of each graph in this dict 
       self.graphs_measures[nameDB]=pd.concat([measure_type1.T, measure_type2.T, measure_type3.T])

     
    
      
    # Execute the queries of a certain type and for a certain number of iteration
    def executeQueries(self, tx, set, typeNumber): 
        measuresType = pd.DataFrame({
                            f"Type{typeNumber}_query1":{"Type":None, "query":None, "Mean":None, "Std":None, "Median":None}, 
                            f"Type{typeNumber}_query2":{"Type":None, "query":None, "Mean":None, "Std":None, "Median":None},
                            f"Type{typeNumber}_query3":{"Type":None, "query":None, "Mean":None, "Std":None, "Median":None},
                            f"Type{typeNumber}_query4":{"Type":None, "query":None, "Mean":None, "Std":None, "Median":None}
                        })

        
        for i , query in enumerate(set):

            # Saving the measured time
            execution_time = []

            for _ in range(self.numberOfIteration):
                start = timer()
                result= tx.run(query)
                end = timer()

                # Saving the time in microsecond
                key = f"Type{typeNumber}_query{i+1}"
                execution_time.append( (end-start) *1e6)

            # Calculate the measures and save them
            measuresType[key]["Type"]=typeNumber
            measuresType[key]["query"] = f"query_{i +1}"
            measuresType[key]["Mean"] = np.mean(execution_time)
            measuresType[key]["Median"] = np.median(execution_time)
            measuresType[key]["Std"] = np.std(execution_time)


        return measuresType

    # Get the queries for the file
    def getQueriesFromFile(self,file):
        return pd.read_excel(file)
    
    # Create for every graph a cvs file
    def convertResults(self): 
        for db in self.databases:
            df = self.graphs_measures[db]
            df.to_csv(f"./experiment_1/{db}_measures", index=False)
            


if __name__ == "__main__":
    o = experiments()
    o.experiment("./experiment_1.xlsx",10)
    o.convertResults()




