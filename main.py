from experiments import performanceTest
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

def average_response_time(experiment):
        
        for i, model in enumerate(["grundriss2", "grundriss6", "grundriss8", "grundriss10"]): 

            # find right path of the model
            model_path_LPG =f"{experiment}/results/{model}.csv"
            model_path_RDF =f"{experiment}/results/{model}RDF.csv"

            # Load data from csv file
            data_LPG = pd.read_csv(model_path_LPG)
            data_RDF = pd.read_csv(model_path_RDF)

            queries = data_LPG['Query']
            X_axis = np.arange(len(queries)) 


            plt.bar(X_axis - 0.2, data_LPG["Average Response Time (µs)"], 0.4,  label = 'LPG') 
            plt.bar(X_axis + 0.2, data_RDF["Average Response Time (µs)"], 0.4,  label = 'RDF') 


            plt.xticks(X_axis, queries) 
            plt.xlabel("Queries") 
            plt.ylabel("Execution Time (µs)") 
            plt.title(f"Model {i+1}: {model}") 
            plt.legend() 
            
            plt.savefig(f"{experiment}/diagrams/{model}.png")
            plt.close()



if __name__ == "__main__":
    iterations = 1000
    #first Experiment 
    e1 = performanceTest(file ="./experiment_1/queries.xlsx", numberOfIteration=iterations)
    e1.executeExperiment()
    e1.convertResults("./experiment_1/results")

    #second Experiment 
    e2 = performanceTest("./experiment_2/queries.xlsx", numberOfIteration=iterations)
    e2.executeExperiment()
    e2.convertResults("./experiment_2/results")

    # Anaysis 
    average_response_time("experiment_1")
    average_response_time("experiment_2")
