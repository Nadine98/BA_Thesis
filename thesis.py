from neo4j import GraphDatabase
import pandas as pd
from time import time as timer 


class experiments: 
    def __init__(self):

        self.databases = ["grundriss2", "grundriss2RDF"]
        
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
       measure_type1 = self.executeQueries(tx,self.queries[self.queries['Type']==1][dbType].to_list())

       # Query of type 2
       measure_type2 = self.executeQueries(tx, self.queries[self.queries['Type']==2][dbType].to_list())

       # Query of type 3
       measure_type3 = self.executeQueries(tx, self.queries[self.queries['Type']==3][dbType].to_list())

       # Safing the result of a graph in this dict
       self.graphs_measures[nameDB]=[measure_type1, measure_type2, measure_type3]
       print(nameDB)
       print(self.graphs_measures[nameDB][0])

                

    # Execute the queries of a certain type and for a certain number of iteration
    def executeQueries(self, tx, set): 
        measuresType = dict({"query1":[], "query2":[], "query3":[], "query4":[]})
        
        for _ in range(self.numberOfIteration):
            for i , query in enumerate(set): 

                start = timer()
                result= tx.run(query)
                end = timer()

             
                # Saving the time in mi
                measuresType[f"query{i+1}"].append( (end-start) *1e6)
        return measuresType

    # Get the queries for the file
    def getQueriesFromFile(self,file):
        return pd.read_excel(file)

if __name__ == "__main__":
    o = experiments(1000)
    o.experiment("./experiment_1.xlsx")




