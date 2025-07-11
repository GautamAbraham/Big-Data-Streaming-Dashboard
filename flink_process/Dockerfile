FROM flink:1.18

WORKDIR /opt/flink/usrlib

RUN apt-get update && \
    apt-get install -y python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip3 install -r requirements.txt

# Create symbolic link for python command
RUN ln -sf /usr/bin/python3 /usr/bin/python

COPY flink_process.py .
COPY config.ini .
COPY flink-sql-connector-kafka-3.2.0-1.18.jar .

# Use --jarfile at runtime to include the Kafka connector
CMD ["flink", "run", "--jarfile", "/opt/flink/usrlib/flink-sql-connector-kafka-3.2.0-1.18.jar", "-py", "/opt/flink/usrlib/flink_process.py", "--jobmanager", "jobmanager:8081"]