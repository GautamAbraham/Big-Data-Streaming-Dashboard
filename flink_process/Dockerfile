# =====================================================
# FLINK 2.0 DOCKERFILE - BUILT FROM SCRATCH FOR OPTIMAL PERFORMANCE
# =====================================================
FROM flink:2.0

# Set working directory
WORKDIR /opt/flink/usrlib

# =====================================================
# SYSTEM DEPENDENCIES FOR FLINK 2.0 + PYFLINK
# =====================================================
RUN apt-get update && \
    apt-get install -y \
    python3 \
    python3-pip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# =====================================================
# PYTHON ENVIRONMENT SETUP
# =====================================================
# Create symbolic links for both python and pip commands (Windows compatibility)
RUN ln -sf /usr/bin/python3 /usr/bin/python && \
    ln -sf /usr/bin/pip3 /usr/bin/pip

# =====================================================
# FLINK 2.0 KAFKA CONNECTOR (SYSTEM LIBRARY)
# =====================================================
# Copy the correct Kafka connector for Flink 2.0 to lib directory for automatic loading
COPY flink-sql-connector-kafka-4.0.0-2.0.jar /opt/flink/lib/

# =====================================================
# PYFLINK 2.0 DEPENDENCIES
# =====================================================
COPY requirements.txt .
RUN pip3 install -r requirements.txt

# =====================================================
# APPLICATION FILES
# =====================================================
COPY flink_process.py .
COPY config.ini .

# Set executable permissions
RUN chmod +x flink_process.py

# =====================================================
# FLINK 2.0 OPTIMIZED STARTUP COMMAND
# =====================================================
CMD ["flink", "run", \
    "-py", "/opt/flink/usrlib/flink_process.py", \
    "--jobmanager", "jobmanager:8081"]