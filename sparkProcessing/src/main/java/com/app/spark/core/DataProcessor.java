package com.app.spark.core;

import com.app.spark.utils.LoadConfig;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.WritePrecision;
import kafka.serializer.StringDecoder;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.*;
public class DataProcessor {
    private static final Logger logger = Logger.getLogger(DataProcessor.class);

    public static void main(String[] args) throws Exception {
        //read Spark and Cassandra properties and create SparkConf
        System.out.println("here iam ");
        Properties prop = LoadConfig.readPropertyFile();
        SparkConf conf = new SparkConf()
                .setAppName(prop.getProperty("com.app.spark.app.name"))
                .setMaster(prop.getProperty("com.app.spark.master"));
        //batch interval of 5 seconds for incoming stream
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        //add check point directory
        //jssc.checkpoint(prop.getProperty("com.app.spark.checkpoint.dir"));

        //read and set Kafka properties
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("zookeeper.connect", prop.getProperty("com.app.kafka.zookeeper"));
        kafkaParams.put("metadata.broker.list", prop.getProperty("com.app.kafka.brokerlist"));
        kafkaParams.put("auto.offset.reset", "smallest");
        //kafkaParams.put("auto.create.topics.enable","true");
        String topic = prop.getProperty("com.app.kafka.topic");
        Set<String> topicsSet = new HashSet<String>();

        topicsSet.add(topic);
        //create direct kafka stream
        JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicsSet
        );

        String token = "FEFE2O_DXfqWCKXD3GHEFRmQhYohPjHQfvplBFJKe22GJd1ayWAN0C0qsLPuZsnY6fk2ldce0EBe0q0ecCEvTg==";
        String bucket = "Wifx";
        String org = "Wifx";

        InfluxDBClient client = InfluxDBClientFactory.create("http://localhost:8086", token.toCharArray());


        List<String> allRecord = new ArrayList<String>();
        final String COMMA = ",";
        logger.info("Starting Stream Processing");
        directKafkaStream.foreachRDD(rdd -> {
            System.out.println("--- Received new data RDD  " + rdd.partitions().size() + " partitions and " + rdd.count() + " records");
            //rdd
            if (rdd.count() > 0) {
                rdd.collect().forEach(rawRecord -> {
                    System.out.println(rawRecord);
                    System.out.println("***************************************");
                    System.out.println(rawRecord._2);
                    String record = rawRecord._2();
                    StringTokenizer st = new StringTokenizer(record, ",");
                    StringBuilder sb = new StringBuilder();

                    while (st.hasMoreTokens()) {
                        String id = st.nextToken(); // Maps a unit of time in the real world. In this case 1 step is 1 hour of time.
                        String time = st.nextToken(); // CASH-IN,CASH-OUT, DEBIT, PAYMENT and TRANSFER
                        String device_id = st.nextToken(); //amount of the transaction in local currency
                        String application_id = st.nextToken(); //  customerID who started the transaction
                        String gateway_id = st.nextToken(); // initial balance before the transaction
                        String modulation = st.nextToken(); // customer's balance after the transaction.
                        String datarate = st.nextToken(); // recipient ID of the transaction.
                        String snr = st.nextToken(); // initial recipient balance before the transaction.
                        String rssi = st.nextToken(); // recipient's balance after the transaction.
                        String freq = st.nextToken(); // dentifies a fraudulent transaction (1) and non fraudulent (0)
                        String f_cnt = st.nextToken(); // flags illegal attempts to transfer more than 200.000 in a single transaction.
                        String latitude = st.nextToken();
                        String longitude = st.nextToken();
                        String altitude = st.nextToken();
                        // Keep only interested columnn in Master Data set.
                        sb.append(gateway_id).append(COMMA).append(datarate).append(COMMA).append(snr).append(COMMA).append(rssi).append(COMMA).append(freq).append(COMMA).append(latitude).append(COMMA).append(longitude);
                        allRecord.add(sb.toString());

                        String data = "mem,host=host1 used_percent=23.43234543";
                        try (WriteApi writeApi = client.getWriteApi()) {
                            writeApi.writeRecord(bucket, org, WritePrecision.NS, sb.toString());
                        }


                    }



                });
                /*
                System.out.println("All records OUTER MOST :" + allRecord.size());
                FileWriter writer = new FileWriter("Master_dataset.csv");
                for (String s : allRecord) {
                    writer.write(s);
                    writer.write("\n");
                }
                System.out.println("Master dataset has been created : ");
                writer.close();

                 */
            }



        });
        //start context
        jssc.start();
        jssc.awaitTermination();
    }
}
