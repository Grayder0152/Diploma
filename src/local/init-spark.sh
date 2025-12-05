apt-get update && apt-get install -y openjdk-11-jre wget tar nano
export SPARK_VERSION=3.4.1
wget https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop3.tgz \
    && tar -xzf spark-$SPARK_VERSION-bin-hadoop3.tgz -C /opt \
    && ln -s /opt/spark-$SPARK_VERSION-bin-hadoop3 /opt/spark \
    && rm spark-$SPARK_VERSION-bin-hadoop3.tgz
export SPARK_HOME=/opt/spark
export PATH=$SPARK_HOME/bin:$PATH
pip install -r /requirements.txt
export PYTHONPATH=$PYTHONPATH:/
export PYARROW_IGNORE_TIMEZONE=1
#python3 /data_preparing.py