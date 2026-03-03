# A Docker image for the Couchbase SDK test performer for Kotlin.
# To build:
#
# 1. Uncomment the fit-performer modules in the parent POM.
# 2. Ensure `transactions-fit-performer` is present in the same directory as `couchbase-jvm-clients`.
# 3. From that parent directory (containing both projects), run:
#      docker build . -f couchbase-jvm-clients/kotlin-fit-performer/Dockerfile
#
# To run:
#   docker run -e LOG_LEVEL=DEBUG -p 8060:8060


# Valid SDK values: java, scala, kotlin, columnar-java
ARG SDK=kotlin

FROM maven:3.9.6-eclipse-temurin-21 AS build

WORKDIR /app

COPY transactions-fit-performer transactions-fit-performer/
COPY couchbase-jvm-clients couchbase-jvm-clients/

ARG MVN_FLAGS="--batch-mode --no-transfer-progress -Dcheckstyle.skip -Dmaven.test.skip -Dmaven.javadoc.skip"

WORKDIR /app/transactions-fit-performer
RUN mvn $MVN_FLAGS --projects gRPC/jvm --also-make clean install

WORKDIR /app/couchbase-jvm-clients
RUN mvn $MVN_FLAGS -f protostellar/pom.xml clean install
RUN mvn $MVN_FLAGS -f core-io-deps/pom.xml clean install
RUN mvn $MVN_FLAGS -f tracing-opentelemetry-deps/pom.xml clean install

# Defer declaring the ARG until first use, so the previous layers can be cached between SDKs
ARG SDK
RUN mvn $MVN_FLAGS package --projects ${SDK}-fit-performer --also-make

# Multistage build to keep things small
FROM eclipse-temurin:21-jre-ubi10-minimal

WORKDIR /app

ARG SDK
COPY --from=build /app/couchbase-jvm-clients/${SDK}-fit-performer/target/${SDK}-fit-performer-1.0-SNAPSHOT-jar-with-dependencies.jar performer.jar

ENV LOG_LEVEL=INFO
EXPOSE 8060

ENTRYPOINT ["java", "-jar", "performer.jar"]
