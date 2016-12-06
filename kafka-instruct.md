CloudxLab Kafka Introduction 



Apache Kafka is publish-subscribe messaging rethought as a distributed commit log. Read more about Apache Kafka here


How to use Kafka in CloudxLab


Apache Kafka scripts are located under /usr/hdp/current/kafka-broker/bin


Login to web console
export PATH=$PATH:/usr/hdp/current/kafka-broker/bin


Create the Topic -


Get one of the zookeeper-connect ip from ambari. Login to ambari using the credentials given under “My Lab” section. Click on “Kafka” on left-hand navigation. Click on “Config” and select one of the ip from “zookeeper.connect” property. Let's say it as zookeeper-connect-ip





kafka-topics.sh --create --zookeeper zookeeper-connect-ip:2181 --replication-factor 1 --partitions 1 --topic test


Check if topic was created - 


kafka-topics.sh --list --zookeeper zookeeper-connect-ip:2181


Get ip address of one of the broker - 


zookeeper-client get /brokers/ids/0


Let's say it as broker-ip


Push message to the topic -


kafka-console-producer.sh --broker-list broker-ip:6667 --topic test


Consume message from the topic -


Open web console in another tab and run these commands to consume the produced messages


export PATH=$PATH:/usr/hdp/current/kafka-broker/bin


kafka-console-consumer.sh --zookeeper zookeeper-connect-ip:2181 --topic test --from-beginning


Please find screencast of the above commands here
