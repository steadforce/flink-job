package com.steadforce.flink_job;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import static org.apache.flink.table.api.Expressions.$;
import com.steadforce.flink_job.JobData;
import java.lang.*;

/**
 * Hello world!
 *
 */
public class App
{
  public static void main(String[] args) throws Exception {

        String nessieHost = System.getenv("NESSIE_HOST");
        String nessiePort = System.getenv("NESSIE_PORT");
        String minioHost = System.getenv("MINIO_HOST");
        String minioPort = System.getenv("MINIO_PORT");
        String warehouse = System.getenv("WAREHOUSE");
        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // set up the table environment
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(
                env,
                EnvironmentSettings.newInstance().inStreamingMode().build());


        // create the Nessie catalog
        tableEnv.executeSql(
                String.format("CREATE CATALOG iceberg WITH ("
                        + "'type'='iceberg',"
                        + "'catalog-impl'='org.apache.iceberg.nessie.NessieCatalog',"
                        + "'io-impl'='org.apache.iceberg.aws.s3.S3FileIO',"
                        + "'uri'='http://%s:%s/api/v1',"
                        + "'authentication.type'='none',"
                        + "'ref'='main',"
                        + "'s3.endpoint'='http://%s:%s'"
                        + "'warehouse' = 's3://%s',"
                        + ")", nessieHost, nessiePort, minioHost, minioPort, warehouse));


        // List all catalogs
        TableResult result = tableEnv.executeSql("SHOW CATALOGS");


        // Print the result to standard out
        result.print();


        // Set the current catalog to the new catalog
        tableEnv.useCatalog("iceberg");


        // Create a database in the current catalog
        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS db");


        // create the table
        tableEnv.executeSql(
                "CREATE TABLE IF NOT EXISTS db.table1 ("
                        + "id BIGINT COMMENT 'unique id',"
                        + "data STRING"
                        + ")");


        // create a DataStream of Tuple2 (equivalent to Row of 2 fields)
        DataStream<Tuple2<Long, String>> dataStream = env.fromElements(
                Tuple2.of(1L, "foo"),
                Tuple2.of(1L, "bar"),
                Tuple2.of(1L, "baz"));


        // apply a map transformation to convert the Tuple2 to an JobData object
        DataStream<JobData> mappedStream = dataStream.map(new MapFunction<Tuple2<Long, String>, JobData>() {
            @Override
            public JobData map(Tuple2<Long, String> value) throws Exception {
                // perform your mapping logic here and return a JobData instance
                // for example:
                return new JobData(value.f0, value.f1);
            }
        });


        // convert the DataStream to a Table
        Table table = tableEnv.fromDataStream(mappedStream, $("id"), $("data"));


        // register the Table as a temporary view
        tableEnv.createTemporaryView("my_datastream", table);


        // write the DataStream to the table
        tableEnv.executeSql(
                "INSERT INTO db.table1 SELECT * FROM my_datastream");
   }
}