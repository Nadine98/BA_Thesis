from experiments import performanceTest

if __name__ == "__main__":
    #first Experiment 
    #e1 = performanceTest(file ="./experiment_1/queries.xlsx", numberOfIteration=1000)
    #e1.executeExperiment()
    #e1.convertResults("./experiment_1/results")
    
    #second Experiment 
    e2 = performanceTest("./experiment_2/queries.xlsx", 1000)
    e2.executeExperiment()
    e2.convertResults("./experiment_2/results")