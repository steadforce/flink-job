FROM flink:1.17.2-scala_2.12-java8

    ## Hadoop Common Classes
RUN curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.6/hadoop-common-3.3.6.jar -o /opt/flink/lib/hadoop-common-3.3.6.jar && \
    ## Hdfs Classes
    curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-hdfs-client/3.3.6/hadoop-hdfs-client-3.3.6.jar -o /opt/flink/lib/hadoop-hdfs-client-3.3.6.jar && \
    ## Iceberg Flink Library
    curl -L https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.17/1.4.3/iceberg-flink-runtime-1.17-1.4.3.jar -o /opt/flink/lib/iceberg-flink-runtime-1.17-1.4.3.jar && \
    ## Hive Flink Library
    curl -L https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-hive-3.1.3_2.12/1.17.2/flink-sql-connector-hive-3.1.3_2.12-1.17.2.jar -o /opt/flink/lib/flink-sql-connector-hive-3.1.3_2.12-1.17.2.jar && \
    ## Hadoop AWS Classes
    curl -L https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop2-uber/2.8.3-1.8.3/flink-shaded-hadoop2-uber-2.8.3-1.8.3.jar -o /opt/flink/lib/flink-shaded-hadoop2-uber-2.8.3-1.8.3.jar && \
    ## AWS Bundled Classes
    curl -L https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.25.1/bundle-2.25.1.jar -o /opt/flink/lib/bundle-2.25.1.jar && \
    ## Kafka Connector
    curl -L https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.17/flink-sql-connector-kafka-3.1.0-1.17.jar  -o /opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.17.jar 

ENTRYPOINT ["/docker-entrypoint.sh"]

CMD ["help"]