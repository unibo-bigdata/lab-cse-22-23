# 301 Spark basics

The goal of this lab is to get familiar with Spark programming.

## 301-1 Cluster and notebook startup

The only configuration differing from 201-1 is the list of services.

- Software
    - Choose the latest EMR version
    - **Services: Hadoop, Hive, JupiterEnterpriseGateway, Spark, Livy**
    - After last step completes: Clusters enters waiting state
- Hardware
    - 1 Master node, On demand, m5.xlarge (default)
    - 2 Core nodes, On demand, m5.xlarge (default)
    - Disable automatic termination
- General
    - Name: whichever you prefer; the same name can be re-used several times
    - Enable log registration
- Security
    - Choose the *Key pair* created in 101-3
    
After creating the cluster, click on "Notebook" on the left panel, then "Create notebook".
- In the configuration parameters, make sure that you associate the notebook to the created cluster.
- As "AWS Service Role" use "**LabRole**"

## 301-2 Running a sample Spark job

Goal: calculate the average temperature for every month; dataset is ```weather-sample1```.

Spark jobs can be run in multiple ways. 
In any case, a Spark application must be activated
with a certain configuration in terms of:

- ```num-executors```: the total number of executors (i.e., YARN containers) to be created
- ```executor-cores```: the number of CPU cores to be assigned to *each* executor
- ```executor-memory```: the amount of memory to be assigned to *each* executor

### Via spark-shell

Interactive applications can be written using the *shell*. 
No need to use an IDE, just write and execute jobs. 
Suitable for exploratory activities and live demos.

- Connect with Putty to the AWS Master node
- Launch the ```spark-shell``` command to run Spark
    - Possibly add configuration parameters, 
    e.g., ```--num-executors 4```
- Take the code from ```material/example-weather.scala```,
change input and output dir, and copy-paste it into the shell

### Via spark-submit

Batch applications can be submitted by invoking the *spark-submit* program. 
Write applications using an IDE (e.g., Intellij), compile and submit the jar. 
Most suitable for production jobs.

- Compile the code in this repo
- Connect with Putty AND WinSCP to the AWS Master node
- Move the JAR to the Master node with WinSCP
- Submit with ```spark-submit --class <mainClass> <jarFile> <inputDir> <outputDir>``` to run on Spark
    - ```spark-submit --class ExampleWeather BD-301-spark-basics.jar s3a://unibo-bd2122-egallinucci/datasets/weather-sample1.txt s3a://unibo-bd2122-egallinucci/spark/301-2```
    - Possibly add configuration parameters (e.g., ```--num-executors 4```) **before** the ```<jarFile>```
    

*NOTE*: Spark libraries are heavy; use the fat JAR *only if* you need some library that is not in the cluster.

### Via Notebook

The notebook provides a shell-like interaction mode, but more powerful. 
You can create, save, run and re-run scripts whenever necessary,
 and you can alternate code snippets with markdown text.

- Open the notebook created in 301-1
- Upload the ```material/301.ipynb``` Spark notebook file
- Run the snippets in the 301-2 section
  - Remember to update input and output directories

## 301-3 & 301-4

Follow the 301-3 and 301-4 sections in the notebook.
<!--
Launch the Spark shell and load the ```capra``` and ```divinacommedia``` datasets.

```
val rddCapra = sc.textFile("hdfs:/bigdata/dataset/capra/capra.txt")
val rddDC = sc.textFile("hdfs:/bigdata/dataset/divinacommedia")
```

Try the following actions:
- Show their content (```collect```)
- Count their rows (```count```)
- Split phrases into words (```map``` or ```flatMap```; what’s the difference?)
- Check the results (remember: evaluation is lazy)

## 301-3 From MapReduce to Spark

Reproduce on Spark the exercises done on Hadoop MapReduce on the capra and divinacommedia datasets.

- Jobs:
  - Count the number of occurrences of each word
    - Result: (sopra, 1), (la, 4), …
  - Count the number of occurrences of words of given lengths
    - Result: (2, 4), (5, 8)
  - Count the average length of words given their first letter (hint: check the example in 301-1)
    - Result: (s, 5), (l, 2), …
  - Return the inverted index of words
    - Result: (sopra, (0)), (la, (0, 1)), …
- How does Spark compare with respect to MapReduce? (performance, ease of use)
- How is the output sorted? How can you sort by value?
-->

## 301-e Testing

Testing allows to verify Spark code without 
deploying the application to the cluster.
Check the code under ```test/scala/TestSpark.scala```.