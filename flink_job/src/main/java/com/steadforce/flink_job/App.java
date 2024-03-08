package com.steadforce.flink_job;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import static org.apache.flink.table.api.Expressions.$;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Iterator;
import java.util.Map;
/**
 * Hello world!
 *
 */

public class App
{
 public static boolean isManipulatedRow(String jsonString, Random random) {
        return random.nextBoolean();
    }

public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromArgs(args);

        String kafkaBootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        String kafkaTopic = System.getenv("KAFKA_TOPIC");
        String kafkaSchemaRegistryUrl = System.getenv("KAFKA_SCHEMA_REGISTRY_URL");
        String kafkaConsumerGroup = System.getenv("KAFKA_CONSUMER_GROUP");

        String nessieHost = System.getenv("NESSIE_HOST");
        String warehouse = System.getenv("WAREHOUSE");
        String minioHost = System.getenv("MINIO_HOST");

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        // set up the table environment
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(
                env,
                EnvironmentSettings.newInstance().inStreamingMode().build());

        // Create KafkaSource builder
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBootstrapServers)
                .setGroupId(kafkaConsumerGroup)
                .setTopics(kafkaTopic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // create the Nessie catalog
        tableEnv.executeSql(
                String.format(
                "CREATE CATALOG iceberg WITH ("
                        + "'type'='iceberg',"
                        + "'catalog-impl'='org.apache.iceberg.nessie.NessieCatalog',"
                        + "'io-impl'='org.apache.iceberg.aws.s3.S3FileIO',"
                        + "'uri'='%s',"
                        + "'authentication.type'='none',"
                        + "'ref'='main',"
                        + "'client.assume-role.region'='us-east-1',"
                        + "'warehouse'='%s',"
                        + "'s3.endpoint'='%s'"
                        + ")", nessieHost, warehouse, minioHost));

        // List all catalogs
        TableResult result = tableEnv.executeSql("SHOW CATALOGS");

        // Print the result to standard out
        result.print();

        // Set the current catalog to the new catalog
        tableEnv.useCatalog("iceberg");

        // Create a database in the current catalog
        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS db");

        // Add Kafka source as a data source
        DataStream<String> kafkaStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // // Filter manipulated and complete rows
        Random random = new Random();
        DataStream<String> manipulatedRowsStream = kafkaStream.filter(row -> isManipulatedRow(row, random));
        DataStream<String> completeRowsStream = kafkaStream.filter(row -> !isManipulatedRow(row, random));
        // apply a map transformation to convert the Tuple2 to an JobData object
        DataStream<JobData> mappedManipulatedStream = manipulatedRowsStream.map(new MapFunction<String, JobData>() {
            @Override
            public JobData map(String value) throws Exception {
                // perform your mapping logic here and return a JobData instance
                // for example:
                return new JobData(random.nextLong(), value);
            }
        });

        DataStream<JobData> mappedCompleteStream = completeRowsStream.map(new MapFunction<String, JobData>() {
            @Override
            public JobData map(String value) throws Exception {
                // perform your mapping logic here and return a JobData instance
                // for example:
                return new JobData(random.nextLong(), value);
            }
        });

        // Convert the DataStream to a Table
        Table manipulated_table = tableEnv.fromDataStream(mappedManipulatedStream, $("id"), $("data"));
        Table complete_table = tableEnv.fromDataStream(mappedCompleteStream,$("id"), $("data"));

        // Register the Table as a temporary view
        tableEnv.createTemporaryView("my_complete_table", complete_table);
        tableEnv.createTemporaryView("my_manipulated_table", manipulated_table);

        // Create the tables
        tableEnv.executeSql(
               "CREATE TABLE IF NOT EXISTS db.complete_table ("
                       + "id BIGINT COMMENT 'unique id',"
                       + "data STRING"
                       + ")");
        tableEnv.executeSql(
               "CREATE TABLE IF NOT EXISTS db.manipulated_table ("
                      + "id BIGINT COMMENT 'unique id',"
                       + "data STRING"
                       + ")");
        // Write Table to Iceberg

       
        // // Write Table to Iceberg
        manipulated_table.executeInsert("db.manipulated_table");
        complete_table.executeInsert("db.complete_table");
        
        // Write the DataStream to the tables
        // tableEnv.executeSql(
        //        "INSERT INTO db.complete_table SELECT * FROM my_complete_table");

        // tableEnv.executeSql(
        //        "INSERT INTO db.manipulated_table SELECT * FROM my_manipulated_table");

        
        System.print.out("test");
        // Execute the job
        env.execute("Flink Job");
   }

   
}