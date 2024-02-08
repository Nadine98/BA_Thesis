from experiments import performanceTest

if __name__ == "__main__":
    # first Experiment 
    e1 = performanceTest.executeExperiment("./Experiment_1/queries.xlsx", 1000)
    # second Experiment 
    e2 = performanceTest.executeExperiment("./Experiment_2/queries.xlsx", 1000)