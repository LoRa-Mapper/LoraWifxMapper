#mvn clean install
#cp target/simple_kafka_mqtt_connector-0.0.1-SNAPSHOT-jar-with-dependencies.jar  src/main/resources/simple_kafka_mqtt_connector-0.0.1-SNAPSHOT-jar-with-dependencies.jar
cd src/main/resources || exit
java -jar simple_kafka_mqtt_connector-0.0.1-SNAPSHOT-jar-with-dependencies.jar

