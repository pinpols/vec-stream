# Iceberg REST catalog + Postgres JDBC 驱动(默认 fixture 只带 SQLite,并发不稳)。
FROM apache/iceberg-rest-fixture:1.9.2
USER root
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /usr/lib/iceberg-rest/postgresql.jar
RUN chmod 644 /usr/lib/iceberg-rest/postgresql.jar
USER iceberg
WORKDIR /usr/lib/iceberg-rest
ENTRYPOINT ["java","-cp","iceberg-rest-adapter.jar:postgresql.jar","org.apache.iceberg.rest.RESTCatalogServer"]
