FROM spark:3.5.7-java17
LABEL authors="kd"
VOLUME /tmp
ENV SPARK_HOME=/opt/spark
WORKDIR /app


ENV PATH=$SPARK_HOME/bin:$PATH
ENV S3_URL=http://rustfs.orb.local:9000
ENV CATALOG_URL=http://iceberg-rest.orb.local:8181
ENV MAX_BYTES_PER_TRIGGER=485760
ENV TRIGGER_TYPE=ONCE
ENV IGNITE_ENDPOINT=jdbc:ignite:thin://127.0.0.1:10800/
ENV ENRICHED_SCHEMA_PATH=/app/resources/avsc/enriched_teams.avsc
ENV SOURCE_SCHEMA=/app/resources/avsc/team.avsc
ENV S3_ACCESS_KEY=rustfsadmin
ENV S3_SECRET_KEY=rustfsadmin
ENV FILE_NAME=teams.avro

ENV KAFKA_BOOTSTRAP_SERVER=http://kafka-broker.orb.local:29092

ENV SPARK_HOME=/opt/spark
ENV SPARK_DEPLOY_MODE=cluster
ENV SPARK_MASTER=k8s://https://kubernetes.default.svc:443
ENV SPARK_IMAGE=local/spark-ignite:v1
ENV SPARK_S3_URL=http://rustfs.orb.local:9000
ENV SPARK_CATALOG_URL=http://iceberg-rest.orb.local:8181

COPY ./target/teams-scaler-1.0.jar /app/teams-scaler-1.0.jar
COPY ./target/resources/application-prod.properties /app/resources/application-prod.properties

ENTRYPOINT java -jar /app/teams-scaler-1.0.jar --spring.profiles.active=prod

#
#ENTRYPOINT ["java", "-jar", "myapp.jar",  "--spring.profiles.active=prod"]