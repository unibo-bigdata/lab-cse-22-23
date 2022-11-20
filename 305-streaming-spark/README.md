# 305 Spark Streaming

The goal of this lab is to acquire familiarity with streaming data analysis in Spark.

- [Spark Streaming Guide](https://spark.apache.org/docs/latest/streaming-programming-guide.html)

## 305-0 Setup

For the streaming scenario, we need a streaming source. 
It can be done in two ways:

- A Netcat server instantiated on the cluster that sends some data
- A Kafka cluster to collect and distribute messages

Due to limitations on AWS Academy, we rely on the first option. (However, a sample application that produces messages and pushes them on Kafka is in folder 305-streaming-kafka-producer.)

To produce messages through a Netcat server:

- Connect to your cluster's Master node
- Run a Netcat server with ```nc -lk <port>```, where ```<port>``` is an available port number (e.g., 9999)
- Now, anything you write (or copy/paste) will be sent to any client connecting to the Netcat server

### Deploy streaming application via spark-submit

This is the best way to run streaming applications.

- Open the Spark project with IntelliJ
- Compile
- Use WinSCP to put the jar file on the Master node
- Connect to the Master node (using a different connection than the one used to create the Netcat server) and run of the following commands:
  - With the Netcat server, use ```spark-submit --class ExerciseNetcat BD-305-streaming-spark.jar <exerciseNumber> <host> <port>```
    - ```<host>``` is the address of the Master node
    - ```<port>``` is the same entered above
  - With Kafka, use ```spark-submit --jars spark-streaming-kafka-0-10_2.11-2.1.0.jar --class ExerciseKafka BD-305-streaming-spark.jar <exerciseNumber> <consumerGroup> <topicName>``` 
    - ```<consumerGroup>``` is the name of the consumer group 
      - Use your username as the consumer group
    - ```<topicName>``` (optional) is the topic from which messages will be consumed
      - Avoid conflicts by defining topic names in the form "username_myTopic"
    - The ```spark-streaming-kafka-0-10_2.11-2.1.0.jar``` file is provided with this package, in the ```resources``` folder;
make sure that you put this file in the local directory of your cluster machine
      - Note: with a fat jar there would be no need, but the fat jar with Spark's libraries is VERY heavy

Some exercises require additional parameters; in such cases, proper usage is indicated in the exercises' description.

#### Troubleshooting

*Too many INFO messages in Spark Streaming application.* Connect to the Master node and 
```sudo nano /usr/lib/spark/conf/log4j.properties``` to change 
the ```log4j.logger.org.apache.spark.streaming``` property
to ```ERROR```. For different methods, check 
[this link](https://stackoverflow.com/questions/27781187/how-to-stop-info-messages-displaying-on-spark-console).

*The connection dropped when the Netcat server was running;
now, when you reconnect, the port is occupied.* 
Find running processes with ```ps aux | grep "nc -lk"``` 
and kill them with ```kill <pid>```.

*The connection dropped when the Spark streaming app was running;
now, when you launch a new one, it is reported as "accepted" but it
never starts.* 
It's because the old application is still running an YARN has no available resource to start the application. Open an SSH tunnel to port 8088, connect to 
[127.0.0.1:8088](127.0.0.1:8088) and find the id of the running
application (e.g., "application_0123456789"). 
Then, connect to the Master node and run 
```yarn application -kill <appId>```.

*You get a NoSuchMethodError when running the spark-submit 
with Kafka.* Run the following command: ```export SPARK_KAFKA_VERSION=0.10```



### Deploy streaming application via spark-shell

The Spark shell is not the best tool for the streaming scenario.
- Once a StreamingContext has been stopped, it cannot be restarted. 
- The whole application must be rewritten every time.
- If the SparkContext is accidentally stopped, the shell must be restarted as well.

Important: call ```ssc.stop(false)``` to stop the StreamingContext while leaving the SparkContext alive. It may happen however that Spark does not close correctly the StreamingContext; 
in such case, the shell must be restarted.


## 305-1 Testing the streaming application

[Netcat users] Copy/Paste some content (e.g., the text from the ```divinacommedia.txt```
file) into the Netcat server and see what happens on the application's console.

[Kafka users] Produce some textual content (productionMode = 2) 
and see what happens on the application's console. Default topic: ```bigdata_quotes```

## 305-2 Word count

Same as above.

## 305-3 Enabling checkpoint

Checkpoints allow the application to restart from where it last stopped.
               
NOTICE: you need to create a directory on S3 to store the checkpoint data.
For instance, ```s3a://unibo-bd2122-egallinucci/streaming/cp3```.

Then run the application; you need to provide 
an additional ```<path>``` parameter to the ```spark-submit``` command
to indicate the absolute path on S3, e.g., ```/user/egallinucci/streaming/checkpoint3```

[Kafka users] Default topic: ```bigdata_quotes```

## 305-4 Enabling State

State allows the job to continuously update a temporary result (i.e., the state).
   
NOTICE: you need to either
- create a DIFFERENT directory on S3 to store the checkpoint data
- empty the previous directory

Otherwise, the application will re-run the job already checkpointed in the directory!

The additional ```<path>``` parameter is required.

[Kafka users] Default topic: ```bigdata_quotes```

## 305-5 Sliding windows

This job carries out word counting on a sliding window 
that is wide 30 seconds and is updated every 3 seconds.

[Kafka users] Default topic: ```bigdata_quotes```

## 305-6 Trending hashtags

The dataset for this exercise (and for the next ones) 
is the content of ```dataset/tweet.dsv```, 
which contains a set of public tweets that discussed the topic
of vaccines back in 2016.

- [Netcat users] Copy/Paste some content from the ```dataset/tweet.dsv``` file.
- [Kafka users] Produce tweets (productionMode = 1).
 
This job is a simple evolution of word counting to carry out hashtag 
counting via sliding windows. 
The window is wide 1 minute and it is updated every 5 seconds.

[Kafka users] Default topic: ```bigdata_tweets```

## 305-7 Incremental count of tweets by city

This is a stateful job to incrementally count the number of tweets by city. 

Remember to either create a new directory on S3 or to empty the previous one.
The additional ```<path>``` parameter is required.

[Kafka users] Default topic: ```bigdata_tweets```

## 305-8 Incremental count of tweets by city

This job extends the previous one by calculating also the average sentiment 
(per country instead of per city).

Remember to either create a new directory on S3 or to empty the previous one. 
The additional ```<path>``` parameter is required.

[Kafka users] Default topic: ```bigdata_tweets```