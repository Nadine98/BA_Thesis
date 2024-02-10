from experiments import performanceTest

if __name__ == "__main__":
    # first Experiment 
    e1 = performanceTest(file ="./experiment_1/queries.xlsx", numberOfIteration=1001)
    e1.executeExperiment()
    e1.convertResults()
    
    # second Experiment 
  #  e2 = performanceTest("./Experiment_2/queries.xlsx", 11)