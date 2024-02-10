from neo4j import GraphDatabase
import pandas as pd
from tqdm import tqdm
import numpy as np
from time import time as timer 
import psutil
import os



class performanceTest: 
    def __init__(self, file, numberOfIteration):

        self.databases = ["grundriss2", "grundriss2RDF", "grundriss6", "grundriss6RDF", "grundriss8", "grundriss8RDF", "grundriss10", "grundriss10RDF"]
        
        # For connecting to the Neo4j Database Management Sytsem
        self.__URI = "bolt://localhost:7687"
        self.__AUTH = ("neo4j", "kais1234")

        # Adding the execution times for graph (LPG and RDF graph)
        self.graphs_measures={}
    
        # Number for running each query again -> to get a stable result
        self.numberOfIteration = numberOfIteration
        self.file = file 



    def executeExperiment(self):
    
        self.numberOfIteration = self.numberOfIteration

        with  GraphDatabase.driver(self.__URI, auth=self.__AUTH) as driver:
            for i in range(len(self.databases)):
                if "RDF" in self.databases[i]: 
                    dbType = "RDF"
                else:
                    dbType = "LPG"

                with driver.session(database=self.databases[i]) as session:
                    self.queries =self.getQueriesFromFile(self.file)

                    # Execute first experiment
                    if "1" in self.file:
                        session.execute_read(
                            self.setup, 
                            dbType, 
                            self.databases[i]
                        )
                
                    # Execute second experiment
                    if "2" in self.file:
                         session.execute_write(
                            self.setup, 
                            dbType, 
                            self.databases[i]
                        )
                        
       
    # Extract queries according their types and run the queries against each graph
    def setup(self, tx, dbType, dbName):
       
       # Query of type 1
       measure_type1 = self.executeQueries(tx,self.queries[self.queries['Type']==1][dbType].to_list(), 1, dbName)
       # Query of type 2
       measure_type2 = self.executeQueries(tx, self.queries[self.queries['Type']==2][dbType].to_list(), 2,dbName)
       # Query of type 3
       measure_type3 = self.executeQueries(tx, self.queries[self.queries['Type']==3][dbType].to_list(), 3, dbName)

       # Safe results of each graph in this dict 
       self.graphs_measures[dbName]=pd.concat([measure_type1.T, measure_type2.T, measure_type3.T])
      
    
      
    # Execute the queries (of a specifc type) and for a certain number of iterations
    def executeQueries(self, tx, set, typeNumber, dbName): 
        measuresType = pd.DataFrame({
                            f"Type{typeNumber}_query1":{"Type":None, "query":None, "Total Execution Time":None, "Average Response Time":None, "Memory Usage":None}, 
                            f"Type{typeNumber}_query2":{"Type":None, "query":None, "Total Execution Time":None, "Average Response Time":None, "Memory Usage":None},
                            f"Type{typeNumber}_query3":{"Type":None, "query":None, "Total Execution Time":None, "Average Response Time":None, "Memory Usage":None},
                            f"Type{typeNumber}_query4":{"Type":None, "query":None, "Total Execution Time":None, "Average Response Time":None, "Memory Usage":None}
                        })

        
        for i , query in enumerate(set):

            execution_times = [] # For saving the execution time in each iteration
            memory_usages = []

            with tqdm(total=self.numberOfIteration, desc=f"Graph: {dbName}, Type: {typeNumber},  Query: {i+1}", unit="query") as pbar:
                for _ in range(self.numberOfIteration):

                    memory_before, start_time = self.measure_usage()
                    tx.run(query)
                    memory_after, end_time = self.measure_usage()

                    key = f"Type{typeNumber}_query{i+1}"
                    execution_times.append( (end_time-start_time) *1e6) # execution time in microseconds
                    memory_usages.append(memory_after-memory_before) # in bytes 
                    pbar.update()
        

            measuresType[key]["Type"] = typeNumber
            measuresType[key]["query"] = f"query_{i+1}"
            measuresType[key]["Average Response Time"] = np.mean(execution_times)
            measuresType[key]["Total Execution Time"] = np.median(execution_times)
            measuresType[key]["Memory Usage"] = np.mean(memory_usages)

        print(measuresType)
        return measuresType

    # Get the queries form the file
    def getQueriesFromFile(self,file):
        return pd.read_excel(file)
    
    # Create a cvs file with the results for each graph
    def convertResults(self):
        for db in self.databases:
            df = self.graphs_measures[db]
            df.to_csv(f"./experiment_1/results/{db}_measures", index=False)

    def measure_usage(self):
        p = psutil.Process(os.getpid())
        return p.memory_full_info().rss ,timer()
        

