package com.steadforce.flink_job;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.types.Types;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

import org.apache.flink.table.api.Table;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import static org.apache.flink.table.api.Expressions.$;
import java.util.Random;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class App
{
    public static boolean isManipulatedRow(String jsonString) {
        JsonElement jsonElement = JsonParser.parseString(jsonString);
            if (jsonElement.isJsonObject()) {
                JsonObject jsonObject = jsonElement.getAsJsonObject();
                for (String key : jsonObject.keySet()) {
                    if (jsonObject.get(key).isJsonNull()) {
                        return true;
                    }
                }
            }
            return false;
    }

    public static void main(String[] args) throws Exception {
        String kafkaBootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        String kafkaTopic = System.getenv("KAFKA_TOPIC");
        String kafkaConsumerGroup = System.getenv("KAFKA_CONSUMER_GROUP");
        String nessieHost = System.getenv("NESSIE_HOST");
        String warehouse = System.getenv("WAREHOUSE");
        String minioHost = System.getenv("MINIO_HOST");

        ParameterTool parameters = ParameterTool.fromArgs(args);
        Configuration hadoopConf = new Configuration();
        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put("uri", nessieHost);
        catalogProperties.put("warehouse", warehouse);
        catalogProperties.put("s3.endpoint", minioHost);
        catalogProperties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        catalogProperties.put("authentication.type", "none");
        catalogProperties.put("ref", "main");
        catalogProperties.put("client.assume-role.region", "us-east-1");

        CatalogLoader catalogLoader = CatalogLoader.custom(
            "iceberg",
            catalogProperties,
            hadoopConf,
            "org.apache.iceberg.nessie.NessieCatalog");
        Schema schema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "data", Types.StringType.get())
        );
        
        Catalog catalog = catalogLoader.loadCatalog();
        TableIdentifier outputTable = TableIdentifier.of(
            "db",
            "table1");
        if (!catalog.tableExists(outputTable)) {
            catalog.createTable(outputTable, schema, PartitionSpec.unpartitioned());
        }
        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        
        // create a DataStream of Tuple2 (equivalent to Row of 2 fields)
        DataStream<Tuple2<Long, String>> source = env.fromElements(
               Tuple2.of(1L, "foo"),
               Tuple2.of(1L, "bar"),
               Tuple2.of(1L, "baz"));
               
        // apply a map transformation to convert the Tuple2 to an JobData object
        DataStream<Row> stream = source.map(new MapFunction<Tuple2<Long, String>, Row>()
        {
            @Override
            public Row map(Tuple2<Long, String> value) throws Exception {
                Row row = new Row(2);
                row.setField(0, value.f0);
                row.setField(1, value.f1);
                return row;
           }
        });

        // Configure row-based append
        FlinkSink.forRow(stream, FlinkSchemaUtil.toSchema(schema))
            .tableLoader(TableLoader.fromCatalog(catalogLoader, outputTable))
            .distributionMode(DistributionMode.HASH)
            .writeParallelism(2)
            .append();

        // Execute the flink app
        env.execute();
        
        // // set up the table environment
        // final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(
        //         env,
        //         EnvironmentSettings.newInstance().inStreamingMode().build()
        // );

        // // Create KafkaSource builder
        // KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
        //         .setBootstrapServers(kafkaBootstrapServers)
        //         .setGroupId(kafkaConsumerGroup)
        //         .setTopics(kafkaTopic)
        //         .setStartingOffsets(OffsetsInitializer.earliest())
        //         .setValueOnlyDeserializer(new SimpleStringSchema())
        //         .build();

        // // create the Nessie catalog
        // tableEnv.executeSql(
        //         String.format(
        //         "CREATE CATALOG iceberg WITH ("
        //                 + "'type'='iceberg',"
        //                 + "'catalog-impl'='org.apache.iceberg.nessie.NessieCatalog',"
        //                 + "'io-impl'='org.apache.iceberg.aws.s3.S3FileIO',"
        //                 + "'uri'='%s',"
        //                 + "'authentication.type'='none',"
        //                 + "'ref'='main',"
        //                 + "'client.assume-role.region'='us-east-1',"
        //                 + "'warehouse'='%s',"
        //                 + "'s3.endpoint'='%s'"
        //                 + ")", nessieHost, warehouse, minioHost));

        // // create the Nessie catalog
        // tableEnv.executeSql(
        //     "CREATE CATALOG iceberg WITH ("
        //             + "'type'='iceberg',"
        //             + "'catalog-impl'='org.apache.iceberg.rest.RESTCatalog',"
        //             + "'io-impl'='org.apache.iceberg.aws.s3.S3FileIO',"
        //             + "'uri'='http://rest:8181',"
        //             + "'client.assume-role.region'='us-east-1',"
        //             + "'warehouse'='s3://warehouse/wh/',"
        //             + "'s3.endpoint'='http://minio:9000'"
        //             + ")");

        // // List all catalogs
        // TableResult result = tableEnv.executeSql("SHOW CATALOGS");

        // // Print the result to standard out
        // result.print();

        // // Set the current catalog to the new catalog
        // tableEnv.useCatalog("iceberg");

        // // Create a database in the current catalog
        // tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS db");

        // // create the table
        // tableEnv.executeSql(
        //        "CREATE TABLE IF NOT EXISTS db.table1 ("
        //                + "id BIGINT COMMENT 'unique id',"
        //                + "data STRING"
        //                + ")");

        // // create a DataStream of Tuple2 (equivalent to Row of 2 fields)
        // DataStream<Tuple2<Long, String>> dataStream = env.fromElements(
        //        Tuple2.of(1L, "foo"),
        //        Tuple2.of(1L, "bar"),
        //        Tuple2.of(1L, "baz"));
        // // apply a map transformation to convert the Tuple2 to an JobData object
        // DataStream<JobData> mappedStream = dataStream.map(new MapFunction<Tuple2<Long, String>, JobData>() {
        //     @Override
        //     public JobData map(Tuple2<Long, String> value) throws Exception {
        //         // perform your mapping logic here and return a JobData instance
        //         // for example:
        //         return new JobData(value.f0, value.f1);
        //     }
        // });

        // // convert the DataStream to a Table
        // Table table = tableEnv.fromDataStream(mappedStream, $("id"), $("data"));

        // // register the Table as a temporary view
        // tableEnv.createTemporaryView("my_datastream", table);

        // // write the DataStream to the table
        // tableEnv.executeSql(
        //         "INSERT INTO db.table1 SELECT * FROM my_datastream");

        // // Add Kafka source as a data source
        // DataStream<String> kafkaStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // // Filter manipulated and complete rows
        // DataStream<String> manipulatedRowsStream = kafkaStream.filter(row -> isManipulatedRow(row));
        // DataStream<String> completeRowsStream = kafkaStream.filter(row -> !isManipulatedRow(row));
        
        // Random random = new Random();
        // // apply a map transformation to convert the Tuple2 to an JobData object
        // DataStream<JobData> mappedManipulatedStream = manipulatedRowsStream.map(new MapFunction<String, JobData>() {
        //     @Override
        //     public JobData map(String value) throws Exception {
        //         // perform your mapping logic here and return a JobData instance
        //         // for example:
        //         return new JobData(random.nextLong(), value);
        //     }
        // });

        // DataStream<JobData> mappedCompleteStream = completeRowsStream.map(new MapFunction<String, JobData>() {
        //     @Override
        //     public JobData map(String value) throws Exception {
        //         // perform your mapping logic here and return a JobData instance
        //         // for example:
        //         return new JobData(random.nextLong(), value);
        //     }
        // });

        // // Convert the DataStream to a Table
        // Table manipulated_table = tableEnv.fromDataStream(mappedManipulatedStream, $("id"), $("data"));
        // Table complete_table = tableEnv.fromDataStream(mappedCompleteStream,$("id"), $("data"));

        // // Register the Table as a temporary view
        // tableEnv.createTemporaryView("my_complete_table", complete_table);
        // tableEnv.createTemporaryView("my_manipulated_table", manipulated_table);

        // // Write Table to Iceberg
        // manipulated_table.executeInsert("db.manipulated_table");
        // complete_table.executeInsert("db.complete_table");

        // Execute the job
        // env.execute("Flink Job");
   }
}