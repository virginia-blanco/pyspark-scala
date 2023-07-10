# Part Three

* First run
  * `--driver-memory 2G --executor-memory 4G --executor-cores 1 --total-executor-cores 20`
  * Your Expectation: it is expected to be the one that performs the worst as it has the lower memory and the lower number of executors.
  * Your results/runtime: 1580.20 s -> 26.34 min

  ![firstRun](../images/part-three/firstRun.png "firstRun")

* Second run
  * `--driver-memory 10G --executor-memory 12G --executor-cores 2`
  * Your Expectation: it is expected to be the one that performs better as it has the highest value of memory, and as the total executor cores number is not defined, it could try to take as many executor cores as possible. 
  * Your results/runtime: 1947.80 s -> 32.46 min

  ![secondRun](../images/part-three/secondRun.png "secondRun")

* Third run
  * `--driver-memory 4G --executor-memory 4G --executor-cores 2 --total-executor-cores 40`
  * Your Expectation: it is expected to be the one that is in the middle in performance terms
  * Your results/runtime: 1820.35 s -> 30.34 min

  ![thridRun](../images/part-three/thridRun.png "thridRun")

The results may be biased due to queue waits that have not allowed the three runs to have the same execution circumstances, knowing that it is probable that the total amount of memory and executor cores available improves the performance of Spark applications up to a certain point, but increasing them beyond that point can lead to diminishing returns or even reduced performance.

In the first run, the total available memory and executor cores were relatively low, which likely resulted in suboptimal performance.

In the second run, the total available memory and executor cores were significantly higher, but the total number of executor cores was not specified, which means that Spark could only allocate a limited number of executor cores. This could have limited the parallelism of the application and resulted in lower performance than expected.

In the third run, the total available memory and executor cores were increased compared to the first run, but the total number of executor cores was also increased to 40. This likely improved the parallelism of the application and led to better performance compared to the first run.

# Explanation

The 40-parquet file is read and schema and records are shown. The time is recorded by the `library time` using the method `perf_counter()` and the subtraction between the beginning of the script and the end of the script, and the before the schema is defined and the end of the script.

There are some concepts to define:
- Driver Memory: amount of memory allocated to the driver program that runs the main function of the Spark application. Being the driver program the one that runs on the master node of the Spark cluster and coordinates the tasks executed on the worker nodes. A high value is suitable for applications with large amounts of data to process as the driver could store more data in memory reducing the need for disk I/O.

- Executor Memory: amount of memory allocated to each executor, being the process that runs on a worker node and the responsible for executing the tasks assigned to it by the driver program. A high value is suitable for larger tasks to be executed as it improves the parallelism and also reduces the need for disk I/O .

- Executor Cores: number of CPU cores allocated to each executor. The executor cores determine the level of parallelism for a Spark application. A high value is suitable for applications that have a large number of tasks to execute as it will increase the parallelism.

- Total Executor Cores: total number of CPU cores allocated to all the executors in the Spark application. A higher value for Total Executor Cores can allow for more parallelism and better utilization of cluster resources, which can lead to improved performance. 