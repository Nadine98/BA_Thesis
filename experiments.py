from neo4j import GraphDatabase
import pandas as pd
from tqdm import tqdm
import numpy as np
import time  



class performanceTest: 
    def __init__(self, file, numberOfIteration):

        # Graphs that are saved in neo4j
        self.databases = ["grundriss2", "grundriss2RDF", "grundriss6", "grundriss6RDF", "grundriss8", "grundriss8RDF", "grundriss10", "grundriss10RDF"]

        # Adding the execution times for graph (LPG and RDF graph)
        self.graphs_metrics={}

        # Number for running each query again -> to get a stable result
        self.numberOfIteration = numberOfIteration

        # File where the queries are stored
        self.file = file 


    def executeExperiment(self):
        URI = "bolt://localhost:7687"
        AUTH = ("neo4j", "kais1234")

        with  GraphDatabase.driver(URI, auth=AUTH) as driver:
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

    def setup(self, tx, dbType, dbName):
        measures = self.executeQueries(tx, self.queries[dbType], dbName)
        self.graphs_metrics[dbName]= measures.T

    
    def executeQueries(self, tx, set, dbName):
            
            if "1" in self.file:
                measuresType = pd.DataFrame({
                    "Query1":{"Query":None, "Minimum Execution Time (µs)": None, "Maximum Execution Time (µs)": None, "Average Response Time (µs)": None},
                    "Query2":{"Query":None, "Minimum Execution Time (µs)": None, "Maximum Execution Time (µs)": None, "Average Response Time (µs)": None}, 
                    "Query3":{"Query":None, "Minimum Execution Time (µs)": None, "Maximum Execution Time (µs)": None, "Average Response Time (µs)": None},
                    "Query4":{"Query":None, "Minimum Execution Time (µs)": None, "Maximum Execution Time (µs)": None, "Average Response Time (µs)": None},
                    "Query5":{"Query":None, "Minimum Execution Time (µs)": None, "Maximum Execution Time (µs)": None, "Average Response Time (µs)": None},
                    "Query6":{"Query":None, "Minimum Execution Time (µs)": None, "Maximum Execution Time (µs)": None, "Average Response Time (µs)": None}, 
                    "Query7":{"Query":None, "Minimum Execution Time (µs)": None, "Maximum Execution Time (µs)": None, "Average Response Time (µs)": None},
                    "Query8":{"Query":None, "Minimum Execution Time (µs)": None, "Maximum Execution Time (µs)": None, "Average Response Time (µs)": None},
                    "Query9":{"Query":None, "Minimum Execution Time (µs)": None, "Maximum Execution Time (µs)": None, "Average Response Time (µs)": None},
                    "Query10":{"Query":None, "Minimum Execution Time (µs)": None, "Maximum Execution Time (µs)": None, "Average Response Time (µs)": None}, 
                    "Query11":{"Query":None, "Minimum Execution Time (µs)": None, "Maximum Execution Time (µs)": None, "Average Response Time (µs)": None},
                    "Query12":{"Query":None, "Minimum Execution Time (µs)": None, "Maximum Execution Time (µs)": None, "Average Response Time (µs)": None}
                })
                execution_times={"query1":[], "query2":[], "query3":[], "query4":[], "query5":[], "query6":[], "query7":[], "query8":[], "query9":[], "query10":[], "query11":[], "query12":[]}
            
            else:
                measuresType = pd.DataFrame({
                    "Query1":{"Query":None, "Minimum Execution Time (µs)":None, "Maximum Execution Time (µs)":None, "Average Response Time (µs)":None}, 
                    "Query2":{"Query":None, "Minimum Execution Time (µs)":None, "Maximum Execution Time (µs)":None, "Average Response Time (µs)":None},
                    "Query3":{"Query":None, "Minimum Execution Time (µs)":None, "Maximum Execution Time (µs)":None, "Average Response Time (µs)":None}, 
                    "Query4":{"Query":None, "Minimum Execution Time (µs)":None, "Maximum Execution Time (µs)":None, "Average Response Time (µs)":None},
                    "Query5":{"Query":None, "Minimum Execution Time (µs)":None, "Maximum Execution Time (µs)":None, "Average Response Time (µs)":None}, 
                    "Query6":{"Query":None, "Minimum Execution Time (µs)":None, "Maximum Execution Time (µs)":None, "Average Response Time (µs)":None},
                    
                })
                execution_times={"query1":[], "query2":[], "query3":[], "query4":[], "query5":[], "query6":[]}


            for j in range(self.numberOfIteration):
                with tqdm(total=len(set), desc=f"Graph: {dbName},  Iteration: {j+1}", unit="query") as pbar:
                    for numberQuery, query in enumerate(set): 

                        time_before = time.perf_counter_ns() 
                        results = tx.run(query)
                        time_after = time.perf_counter_ns() 

                        results.consume()
                        ex_time = (time_after-time_before)/1e3

                        execution_times[f"query{numberQuery+1}"].append(ex_time)
                        pbar.update()


            for i, _ in enumerate(set):
                measuresType[f"Query{i+1}"]["Query"] = f"{i+1}"
                measuresType[f"Query{i+1}"]["Average Response Time (µs)"] = np.mean(execution_times[f"query{i+1}"])
                measuresType[f"Query{i+1}"]["Maximum Execution Time (µs)"] = max(execution_times[f"query{i+1}"])
                measuresType[f"Query{i+1}"]["Minimum Execution Time (µs)"] = min(execution_times[f"query{i+1}"])

            return measuresType
    

    # Get the queries form the file
    def getQueriesFromFile(self,file):
        df =  pd.read_excel(file)
        return df[['Type', 'RDF', 'LPG']]
        
    
    # Create a cvs file with the results for each graph
    def convertResults(self, file):
        for db in self.databases:
            df = self.graphs_metrics[db]
            df.to_csv(f"{file}/{db}_measures", index=False)

    