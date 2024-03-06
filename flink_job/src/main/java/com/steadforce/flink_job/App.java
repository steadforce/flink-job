package com.steadforce.flink_job;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.FileSystem;
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
        String warehouse = System.getenv("WAREHOUSE");

        Configuration loadedConfig = GlobalConfiguration.loadConfiguration("/opt/flink/conf");
        FileSystem.initialize(loadedConfig, null);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(loadedConfig);
        // set up the table environment
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(
                env,
                EnvironmentSettings.newInstance().inStreamingMode().build());

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
                        + "'warehouse'='%s'"
                        + ")", nessieHost, warehouse));


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