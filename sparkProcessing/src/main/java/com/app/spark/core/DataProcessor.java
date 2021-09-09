package com.app.spark.core;

import com.app.spark.utils.LoadConfig;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import kafka.serializer.StringDecoder;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.time.Instant;
import java.util.*;
public class DataProcessor {
    private static final Logger logger = Logger.getLogger(DataProcessor.class);

    public static void main(String[] args) throws Exception {

        //read Spark and Cassandra properties and create SparkConf
        Properties prop = LoadConfig.readPropertyFile();
        SparkConf conf = new SparkConf()
                .setAppName(prop.getProperty("com.app.spark.app.name"))
                .setMaster(prop.getProperty("com.app.spark.master"));
        //batch interval of 5 seconds for incoming stream
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        //add check point directory
       // jssc.checkpoint(prop.getProperty("com.app.spark.checkpoint.dir"));

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
                    System.out.println("***************************************");
                   // System.out.println(rawRecord._2);
                    String record = rawRecord._2();
                    /* * *rxInfo***/
                    JSONObject json = new JSONObject(record);
                    JSONArray rxInfo = json.getJSONArray("rxInfo");

                    /* ***GatewayID*** */
                    String gateway_id="";
                    gateway_id = rxInfo.getJSONObject(0).getString("gatewayID");
                    System.out.println("gatewayid:"+gateway_id);

                    /* ***txInfo*** */
                    JSONObject txInfo = json.getJSONObject("txInfo");

                    /* ***SpreadingFactor*** */
                    JSONObject loRaModulationInfo =  txInfo.getJSONObject("loRaModulationInfo");
                    int spreading_factor=loRaModulationInfo.getInt("spreadingFactor");
                    System.out.println("spreadingFactor:"+spreading_factor);

                    /* ***Frequency*** */
                    double frequency =  (txInfo.getDouble("frequency"))/1000000;
                    System.out.println("Frequency :"+frequency);

                    String obj = json.getString("objectJSON");
                    JSONObject data=new JSONObject();
                    try {
                         data = new JSONObject(obj);
                    }catch (JSONException err){
                        err.printStackTrace();
                    }
                    /* ***RSSI*** */
                    int rssi= data.getInt("_rssi");
                    System.out.println("rssi :"+rssi);

                    /* ***SNR*** */
                    int snr= data.getInt("_snr");
                    System.out.println("snr  :"+snr);

                    /* ***Latitude*** */
                    double latitude= data.getDouble("_latitude");
                    System.out.println("latitude  :"+latitude);

                    /* ***longitude*** */
                    double longitude= data.getDouble("_longitude");
                    System.out.println("longitude  :"+longitude);

                    /* *** event ID*** */
                    int id= data.getInt("uplink");
                    System.out.println("id  :"+id);

                    /* store data point */
                    Point point = Point
                            .measurement("mem")
                           // .addField("id",uplinkID)
                            .addField("gateway_id",gateway_id)
                            .addField("data_rate", spreading_factor)
                            .addField("frequency",frequency)
                            .addField("rssi", rssi)
                            .addField("snr", snr)
                            .addField("latitude", latitude)
                            .addField("longitude", longitude)
                            .addField("id", id)
                            .time(Instant.now(), WritePrecision.NS);

                    try (WriteApi writeApi = client.getWriteApi()) {
                        writeApi.writePoint(bucket, org, point);
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
