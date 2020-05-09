# Process 2 : Event based trigger from Kafka to copy modified data from Postgres to S3, and to Redshift

### Run steps:
login to ec2 instance and run this: 

`java -jar  /home/ubuntu/Redshift-write-1.0-SNAPSHOT-jar-with-dependencies.jar /home/ubuntu/application.properties`


#### Design for the following below:
This is a pictorial representation of what the code does


![Process 2 image](https://github.com/navcfc/images/blob/master/process%202.png)




