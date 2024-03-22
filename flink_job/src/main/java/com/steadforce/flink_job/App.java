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
        
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);

        // Create KafkaSource builder
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBootstrapServers)
                .setGroupId(kafkaConsumerGroup)
                .setTopics(kafkaTopic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Add Kafka source as a data source
        DataStream<String> kafkaStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // Filter manipulated and complete rows
        DataStream<String> manipulatedRowsStream = kafkaStream.filter(row -> isManipulatedRow(row));
        DataStream<String> completeRowsStream = kafkaStream.filter(row -> !isManipulatedRow(row));
        
        Random random = new Random();

        DataStream<Row> mappedCompleteStream = completeRowsStream.map(new MapFunction<String, Row>()
        {
            @Override
            public Row map(String value) throws Exception {
                Row row = new Row(2);
                row.setField(0, random.nextLong());
                row.setField(1, value);
                return row;
           }
        });

        // Apply a map transformation to convert the Tuple2 to an JobData object
        DataStream<Row> mappedManipulatedStream = manipulatedRowsStream.map(new MapFunction<String, Row>()
        {
            @Override
            public Row map(String value) throws Exception {
                Row row = new Row(2);
                row.setField(0, random.nextLong());
                row.setField(1, value);
                return row;
           }
        });
        String completeTableName = "complete_data";
        String manipulatedTableName = "manipulated_data";
        String schemaName = "db";
        TableIdentifier completeDataTable = TableIdentifier.of(
            schemaName,
            completeTableName);
        if (!catalog.tableExists(completeDataTable)) {
            catalog.createTable(completeDataTable, schema, PartitionSpec.unpartitioned());
        }

        TableIdentifier manipulatedDataTable = TableIdentifier.of(
            schemaName,
            manipulatedTableName);
        if (!catalog.tableExists(manipulatedDataTable)) {
            catalog.createTable(manipulatedDataTable, schema, PartitionSpec.unpartitioned());
        }

        // Configure row-based append
        FlinkSink.forRow(mappedCompleteStream, FlinkSchemaUtil.toSchema(schema))
            .tableLoader(TableLoader.fromCatalog(catalogLoader, completeDataTable))
            .distributionMode(DistributionMode.HASH)
            .writeParallelism(2)
            .append();

        // Configure row-based append
        FlinkSink.forRow(mappedManipulatedStream, FlinkSchemaUtil.toSchema(schema))
            .tableLoader(TableLoader.fromCatalog(catalogLoader, manipulatedDataTable))
            .distributionMode(DistributionMode.HASH)
            .writeParallelism(2)
            .append();

        // Execute the flink app
        env.execute();
   }
}