# 201 MapReduce

The goal of this lab is to create an Elastic Map Reduce (EMR) cluster and use it to run some MapReduce jobs.

## 201-1 Provision an EMR cluster

EMR is AWS's service to provision clusters of machines pre-configured with applications in the Hadoop stack.

From your *Console home*, click on "Services" > "EMR", then "Create cluster".Click on "Advanced options" (top of the page) to be able to set all important configuration parameters.
  - Software
    - Choose the latest EMR version
    - Services: Hadoop (exclude all others)
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

The cluster will be created in 5 to 10 minutes.

### Troubleshooting

*The cluster couldn't be created due to the following error: "The requested instance type m5.xlarge is not supported in the requested availability zone."* In the Hardware configuration, try a different EC2 subnet.

## 201-2 Connect via SSH and WinSCP to the master node

Once the cluster is up-and-running, you first need to allow SSH access to the master node of the newly created EMR cluster.

- From the *EMR service console*, open the list of created clusters, then open the cluster currently running
  - Located the "Master's Public DNS" in the Summary - it will be used later
  - Under "Security and Access", click on the security group of the master node (ElasticMapReduce-master)
    - In the newly opened page, select again the ID of the security group of the master node (ElasticMapReduce-master)
    - Click on "Edit inbound rules"
    - Add an SSH rule that allows connections to port 22 from any IPv4

Now open **Putty** and enter the following configuration.

- Host Name: hadoop@ec2-xxx-xxx-xxx-xxx.compute-1.amazonaws.com
  - Replace "xxx-xxx-xxx-xxx" with the IP address of the "Master's Public DNS"
- Under "Connection" > Expand "SSH" > "Auth", click "Browse" and select your *Key pair*
- *Save the configuration*
- Open the connection

Now open **WinSCP** and enter the following configuration (similar to Putty).

- Server: ec2-xxx-xxx-xxx-xxx.compute-1.amazonaws.com
- User name: hadoop
- Under "Advanced" > "SSH" > "Authentication" enter your *Key pair*
- Open the connection

**Notice**: new clusters will be created in each of the following classes; thus, the IP of the Master's Public DNS will need to be changed as well (in both Putty and WinSCP). 


## 201-3 Connect to services' GUIs through SSH tunnels

Many services (HDFS, YARN, Spark, etc.) expose GUIs with useful information, especially to monitor the execution of jobs and get interesting insights. To access them, SSH tunnels must be enable for each port.

Setup tunnels for ports 8088 (YARN), 19888, and 20888 (Spark).

- Reopen the configuration from 201-2
- Go to "Connection" > Expand "SSH" > "Tunnels"
- For each port XYZ, do the following:
  - Source port: XYZ
  - Destination: ec2-xxx-xxx-xxx-xxx.compute-1.amazonaws.com:XYZ
  - Click "Add"
- *Save the configuration*
- Open the connection

The GUIs are now available under [localhost:8088](), [localhost:19888](), and [localhost:20888]().

## 201-4 Create and run the MapReduce application

This repo contains the code for several jobs. 

First of all, follow these steps to compile the code.

- Take the *AWS Credentials* file from 101 and copy it in the ```src/main/folder``` of this repo.
- Run ```./gradlew```
- You will find the compiled application **BD-201-mapreduce.jar** under ```build/libs```.

>*I'm getting the error "java.lang.IllegalArgumentException: Unsupported class file major version XX".*  
>Try updating gradle to a higher version by changing the file ```gradle/wrapper/gradle-wrapper.properties``` in this repository.

Each job requires the following parameters:

- ```<MainClass>```: the name of the class with the Main you want to run
- ```<s3bucket>```: the URI of the input path's bucket on S3
- ```<inputDir>```: the URI of the S3 folder that contains the input files (or the path to a single file)
- ```<outputDir>```: the URI of the S3 folder to be created by the job to store the results (careful: the application will break if the output path already exists)
- ```<numReducers>```: (optional) the number of Reduce tasks

The Main classes are the following:

- ```exercise1.WordCount```
  - Dataset: capra.txt or divinacommedia.txt
  - Goal: return the frequency of each word
- ```exercise2.WordLengthCount```
  - Dataset: capra.txt or divinacommedia.txt
  - Goal: return the frequency of words with a given length
- ```exercise3.AvgTemperature```
  - Dataset: weather-sample1.txt
  - Goal: return the average temperature by year and month
- ```exercise3.MaxTemperature```
  - Dataset: weather-sample1.txt
  - Goal: return the maximum temperature by year and month
- ```exercise3.MaxTemperatureWithCombiner```
  - Dataset: weather-sample1.txt
  - Goal: as above, but also uses a combiner
- ```exercise4.Ex4AverageWordLength```
  - Dataset: capra.txt or divinacommedia.txt
  - Goal: return the frequency of words with a given first letter
- ```exercise4.Ex4AverageWordLengthWithCombiner```
  - Dataset: capra.txt or divinacommedia.txt
  - Goal: as above, but also uses a combiner
- ```exercise4.Ex4InvertedIndex```
  - Dataset: capra.txt or divinacommedia.txt
  - Goal: return the list of offsets for each word

There are two main ways to run MapReduce jobs: scheduling a "step" on the EMR cluster, or manually launching it.

### Run by scheduling an EMR step

- Take the *BD-201-mapreduce.jar* file and upload it on S3
  - Create a new folder called "jar" under your main bucket
  - Drag and drop the jar file
- On the running cluster page, select the "Steps" tab and add a new step
  - Enter the location of jar file on S3
  - Enter the list of parameters
    - Example: ```exercise1.WordCount s3a://unibo-bd2223-egallinucci s3a://unibo-bd2223-egallinucci/datasets/capra.txt s3a://unibo-bd2223-egallinucci/mapreduce/wordcount-output```
  - In case of failure, indicate to keep the cluster running
- Once you add it, the step will start immediately; however, it will take a long time for the job to start!

### Run via manual launch

- Connect to the cluster via WinSCP (see 201-3) and copy/paste the *BD-201-mapreduce.jar* to ```/home/hadoop```
- Connect to the cluster via SSH (see 201-3)
- Run the following command: ```hadoop jar BD-201-mapreduce.jar <parameters>```, where ```<parameters>``` is the same list from the EMR step above
  - Example: ```hadoop jar BD-201-mapreduce.jar exercise1.WordCount s3a://unibo-bd2223-egallinucci s3a://unibo-bd2223-egallinucci/datasets/capra.txt s3a://unibo-bd2223-egallinucci/mapreduce/wordcount-output```

### Check the results

- Resource Manager: [localhost:8088]()
- History server: [localhost:19888]()
- Take a look at the output files in S3


## 201-5 Understanding MapReduce jobs

Run the various jobs and check the code to understand how they work, what they return and in how much time; focus in particular on ```exercise1.WordCount```, ```exercise4.Ex4InvertedIndex```, and ```exercise3.*```.

- How much time does it take to run the jobs?
- How many mappers and reducers have been instantiated?
- How is the output sorted?
- What happens if we enforce 0 reducers?
- What changes when combiners are used?

## 201-6 Scalability

Focus on ```exercise3.MaxTemperatureWithCombiner``` and execute it on the different weather datasets (weather-sample1.txt, weather-sample10.txt, and weather-full.txt).

- How much time does it take?
- The power of the EMR cluster can be increase by adding new Task nodes
  - Go to the cluster homepage
  - Select the Hardware tab
  - Either *resize* the Core instances, or *add* new Task instances
    - Check [this page](https://aws.amazon.com/it/ec2/pricing/on-demand/) for information about hardware configuration and price for different machine types
    - Resizing/Adding resources requires time (5-10 minutes)



## 201-e Testing

Testing allows to verify the MapReduce code without 
deploying the application to the cluster. However,
this requires a little bit of setup (on Windows).

- Create a directory C:\hadoop\bin
- Download the following files and put them in the above folder (change the Hadoop version to the one of your preference):
  - https://github.com/cdarlint/winutils/raw/master/hadoop-2.7.3/bin/winutils.exe
  - https://github.com/cdarlint/winutils/raw/master/hadoop-2.7.3/bin/hadoop.dll
- Create environment variable HADOOP_HOME=C:\hadoop
- Add $HADOOP_HOME\bin to the PATH environment variable


## Takeaway

At the end of this lab, you should be able to:

- Provision an EMR cluster
- Connect to it via SSH, WinSCP, browser
- Understand and compile MapReduce code
- Run MapReduce applications on EMR
- Understand how MapReduce jobs work