# A Docker image for the Couchbase SDK performance test performer for Java.
# Build from root with:
# docker build -f couchbase-jvm-clients/java-fit-performer/Dockerfile -t java-performer .
# Run with:
# docker run -e LOG_LEVEL=DEBUG -p 8060:8060 java-performer

FROM maven:3.8.1-openjdk-17
WORKDIR /app

COPY transactions-fit-performer transactions-fit-performer/
COPY couchbase-jvm-clients couchbase-jvm-clients/

WORKDIR /app/transactions-fit-performer
RUN mvn -T6 --projects gRPC/jvm --also-make install -Dcheckstyle.skip -Dmaven.test.skip -Dmaven.javadoc.skip

WORKDIR /app/couchbase-jvm-clients
RUN mvn -T6 -f protostellar/pom.xml clean install -Dcheckstyle.skip -Dmaven.test.skip -Dmaven.javadoc.skip
RUN mvn -T6 -f core-io-deps/pom.xml clean install -Dcheckstyle.skip -Dmaven.test.skip -Dmaven.javadoc.skip
RUN mvn -T6 -f tracing-opentelemetry-deps/pom.xml clean install
RUN mvn -T6 package -pl java-fit-performer -am -Dcheckstyle.skip -Dmaven.test.skip -Dmaven.javadoc.skip

# Multistage build to keep things small
FROM maven:3.8.1-openjdk-17

WORKDIR /app
# Default level; override with docker run -e LOG_LEVEL=DEBUG
ENV LOG_LEVEL=INFO

COPY --from=0 /app/couchbase-jvm-clients/java-fit-performer/target/fit-performer-java-sdk-1.0-SNAPSHOT-jar-with-dependencies.jar .
ENTRYPOINT ["java", "-jar", "fit-performer-java-sdk-1.0-SNAPSHOT-jar-with-dependencies.jar"]
