# 101 Setup

The goal of this lab is to enable access to the Virtual Lab on AWS Academy and load some datasets on S3 (that will be used in future classes).

## 101-1 Access the Virtual Lab on AWS Academy

Follow these steps to accees the Virtual Lab on AWS Academy with a 100$ credit.

- Register to the course on [Virtuale](virtuale.unibo.it)
- You will receive an invite to AWS Academy via email (manually sent by the teacher)
- Click on the link received
  - Registration to AWS Academy is necessary
  - No credit card is necessary
- Go to "Modules" > "Learner Lab - Foundational Services"
  - Agree to the terms and conditions
- Click on "Student access"
  - **Bookmark this page**: it will be your entry point for the next times
- Click the "Start Lab" button (top-right corner)
  - The first time, it will take about 5 minutes to startup
  - Next times, it will take only a few seconds
- A **credit counter** will appear, indicating the amount of credit left
- Click on "AWS" (top-left corner) when you have a green light
  - You will now be in your own virtual lab
  - This is the ***Console home*** of the Lab
- **Click the "End Lab"** button (top-right corner) at the end of class
  - If you don't, it should automatically shutdown in 4 hours, but 1) it will consume credit (only EC2 instances are automatically shut down), and 2) better safe than sorry.

### FAQs

*What happens when I end the lab?* 
It depends on the service: for instance, data in S3 will persist (and consume very little credit even when the Lab is down), whereas EMR clusters (and all data within them) will be completely destroyed.

*What happens when I finish my credits?*
You will not be able to use the Lab anymore, not even the data in S3. However, you can request an additional Lab environment to be instantiated on a different account (with a different email), just ask the teacher.



## 101-2 Configure S3 and upload some datasets

Follow these steps to activate the S3 service and start loading some datasets.

- From the *console home*, click on "Services" (top-left corner) > "S3"
- Create a bucket (it is a namespace for folders and files)
  - Its name must be unique within S3
  - Suggestion: "unibo-bd2122-[username]" where [username] is the first letter of your first name, followed by your last name (e.g., "unibo-bd2122-egallinucci")
- Enter the newly created bucket and create a folder called "datasets"
- Enter the newly created folder and upload the following datasets:
  - All file in the "datasets" folder of this repo
  - Download the ZIP file [here](https://big.csr.unibo.it/downloads/bigdata/weather-datasets.zip), unzip on your machine and upload all files

Ultimately, you should end up with the following datasets.

- *capra.txt*: tiny unstructured file with a known Italian riddle (for debugging purposes)
- *divinacommedia.txt*: tiny unstructured file with the full Divina Commedia by Dante Alighieri
- *tweet.dsv*: structured file with 10K public tweets from 2016 about vaccines
- *weather-full.txt*: structured file with weather data from all over the world in year 2000 (collected from the [National Climatic Data Center](ftp://ftp.ncdc.noaa.gov/pub/data/noaa/) of the USA; weights 13GB
  - Sample row: 005733213099999**19580101**03004+51317+028783FM-12+017199999V0203201N00721004501CN0100001N9 **-0021**1-01391102681
  - The date in YYYYMMDD format is located at 0-based position 15-23
  - The temperatue in x10 Celsius degrees is located at 0-based positions 87-92
- *weather-sample10.txt*: a sample of 10% of the data in weather-full.txt; weights 1.3GB
- *weather-sample1.txt*: a sample of 1% of the data in weather-full.txt; weights 130MB
- *weather-stations.csv*: structured file with the description of weather stations collecting the weather data

All these files will continue to be available even after the Lab is ended.




## 101-3 Enable SSH access to the Lab's machines (key pair)

In the following classes you will be required to instantiate machines through the Lab and access them via SSH (e.g., Putty). In order to do it, you will need a Private Key file (.ppk).

From the *Console home*, select the EC2 service, then go to "Network and security" > "Key pairs", then click on "Create a key pair":
  - Put some name (e.g., "bigdata")
  - Choose .ppk as the format
  - Create the key pair > the .ppk file will be automatically downloaded
    - **Do not lose it**, or you will need to create a new key pair!

The downloaded .ppk file will be your **Key pair**.


## 101-4 Enable application access to the Lab's services (access key and secret)

In the following classes you will be required to design applications that access AWS services instantiated in your Lab (e.g., a Spark application that loads data from S3). In order to do it, you will need the access key and secret.

Go to the *Bookmarked page*, click "Aws Details" (top-right corner), then "Show" next to "AWS CLI". Copy-paste the values of aws_access_key_id and aws_secret_access_key (just the values) into a file called *aws_credentials.txt*. These will be your **AWS Credentials**.