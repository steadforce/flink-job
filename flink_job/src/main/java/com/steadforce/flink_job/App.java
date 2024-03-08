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
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
/**
 * Hello world!
 *
 */
public class App
{
public static boolean isManipulatedRow(String row) {
        // Check if the row is null or empty
        if (row == null || row.trim().isEmpty()) {
                return true;
        }

        // Check if the message is empty or contains "null" or "n.a."
        if (row.trim().isEmpty() || row.trim().equalsIgnoreCase("null") || row.trim().equalsIgnoreCase("n.a.")) {
                return true;
        }

                // Create a list of expected field names
        List<String> expectedFieldNames = Arrays.asList("index", "operationendtime", "toolid", "machine", "process",
                "p1datapoint1", "p1datapoint2", "p2datapoint1", "p2datapoint2", "p3datapoint1", "p3datapoint2",
                "p4datapoint1", "p4datapoint2", "p5datapoint1", "p5datapoint2", "p6datapoint1", "p6datapoint2",
                "p7datapoint1", "p7datapoint2", "p8datapoint1", "p8datapoint2");

        try {
                // Parse the JSON row
                ObjectMapper mapper = new ObjectMapper();
                JsonNode jsonNode = mapper.readTree(row);

                // Check if any of the expected fields are missing
                for (String fieldName : expectedFieldNames) {
                if (!jsonNode.has(fieldName)) {
                        return true;
                }
                }
        } catch (Exception e) {
                // Log or handle the exception if necessary
                return true;
        }

        return false;
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
        DataStream<String> manipulatedRowsStream = kafkaStream.filter(row -> isManipulatedRow(row));
        DataStream<String> completeRowsStream = kafkaStream.filter(row -> !isManipulatedRow(row));
        Random random = new Random();
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
        Table complete_table = tableEnv.fromDataStream(mappedCompleteStream, $("value").as("data"));

        // Register the Table as a temporary view
        //tableEnv.createTemporaryView("my_complete_table", complete_table);
        //tableEnv.createTemporaryView("my_manipulated_table", manipulated_table);

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
        manipulated_table.executeInsert("db.manipulated_table");
        // Write Table to Iceberg
        complete_table.executeInsert("db.complete_table");
        // Write the DataStream to the tables
        // tableEnv.executeSql(
        //        "INSERT INTO db.complete_table SELECT * FROM my_complete_table");

        // tableEnv.executeSql(
        //        "INSERT INTO db.manipulated_table SELECT * FROM my_manipulated_table");
        // Execute the job
        env.execute("Flink Job");
   }
}