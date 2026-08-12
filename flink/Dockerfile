# Pinned to Flink 1.17.2 to match apache-flink==1.17.2 in requirements.txt
# (PyFlink's Python package version must match the cluster's Flink version).
FROM flink:1.17.2-scala_2.12-java11

RUN apt-get update && apt-get install -y python3 python3-pip python3-dev curl \
    && ln -s /usr/bin/python3 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /opt/flink/usrlib/requirements.txt
RUN pip3 install --no-cache-dir -r /opt/flink/usrlib/requirements.txt

RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.2-1.17/flink-sql-connector-kafka-3.0.2-1.17.jar \
    && wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.17/flink-connector-jdbc-3.1.2-1.17.jar \
    && wget -P /opt/flink/lib/ https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

COPY job.py parser.py versioning.py /opt/flink/usrlib/
