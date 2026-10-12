# Debezium Connect with the Approach 4 InvoiceLookup SMT installed next to the SQL Server connector
# (so it shares the connector's class loader and its bundled mssql-jdbc driver).
ARG DEBEZIUM_IMAGE=quay.io/debezium/connect:2.7.0.Final
ARG JDK_IMAGE=eclipse-temurin:17-jdk

FROM ${DEBEZIUM_IMAGE} AS connect

FROM ${JDK_IMAGE} AS build
COPY --from=connect /kafka/libs /deps
WORKDIR /build
COPY smt/src ./src
RUN mkdir -p classes \
 && javac --release 11 -cp "/deps/*" -d classes $(find src/main/java -name '*.java') \
 && cp -r src/main/resources/. classes/ \
 && jar cf invoice-lookup-smt.jar -C classes .

FROM ${DEBEZIUM_IMAGE}
COPY --from=build /build/invoice-lookup-smt.jar /kafka/connect/debezium-connector-sqlserver/
