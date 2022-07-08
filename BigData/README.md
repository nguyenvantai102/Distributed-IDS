<h1 align='center'> ✨ BigData Architecture ✨</h1>

- Requirements:
  - Java 11
  - Python 3

<h2 align="center">🛠 Tool 🛠</h2>

<strong> Spark </strong>

```
wget https://www.apache.org/dyn/closer.lua/spark/spark-3.3.0/spark-3.3.0-bin-hadoop3.tgz
tar -xzf spark-3.3.0-bin-hadoop3.tgz
cd ~/spark-3.3.0-bin-hadoop3/conf
cp spark-env.sh.template spark-env.sh
vi spark-env.sh
  MASTER=local[2]
```

<strong> Kafka </strong>

```
wget https://www.apache.org/dyn/closer.cgi?path=/kafka/3.2.0/kafka_2.12-3.2.0.tgz
tar -xzf kafka_2.12-3.2.0.tgz
vi ~/kafka_2.12-3.2.0/config/server.properties
  listeners=PLAINTEXT://<ip>:9092
  advertised.listeners=PLAINTEXT://<ip>:9092
```

<strong> Nprobe </strong>

References: <a src="https://linoxide.com/how-to-install-ntopng-on-ubuntu-20-04/">Nprobe</a>


<h2 align="center">🔥 Launching BigData 🔥</h2>

```
cd ~/spark-3.3.0-bin-hadoop3/
sbin/start-master.sh
sbin/start-slave.sh spark://<ip>:7077

cd ~/kafka_2.12-3.2.0/
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
# Create Topic
bin/kafka-topics.sh --create --bootstrap-server <ip>:9092 --topic topicFlows --partitions 1 --replication-factor 1
# View list topic (cannot run)
bin/kafka-topics.sh --list--bootstrap-server 192.168.1.20:9092 
# View traffic
bin/kafka-console-consumer.sh --topic --bootstrap-server <ip>:9092 topicFlows 

# Collector
sudo nprobe --collector-port 9995 -V 9 -n none -T="%IPV4_SRC_ADDR %PROTOCOL %L7_PROTO %IN_BYTES %OUT_BYTES %IN_PKTS %OUT_PKTS %FLOW_DURATION_MILLISECONDS %TCP_FLAGS %CLIENT_TCP_FLAGS %SERVER_TCP_FLAGS %L7_PROTO_RISK %DURATION_IN %DURATION_OUT %LONGEST_FLOW_PKT %SHORTEST_FLOW_PKT %MIN_IP_PKT_LEN %MAX_IP_PKT_LEN %SRC_TO_DST_SECOND_BYTES %DST_TO_SRC_SECOND_BYTES %RETRANSMITTED_IN_BYTES %RETRANSMITTED_IN_PKTS %RETRANSMITTED_OUT_BYTES %RETRANSMITTED_OUT_PKTS %SRC_TO_DST_AVG_THROUGHPUT %DST_TO_SRC_AVG_THROUGHPUT %NUM_PKTS_UP_TO_128_BYTES %NUM_PKTS_128_TO_256_BYTES %NUM_PKTS_256_TO_512_BYTES %NUM_PKTS_512_TO_1024_BYTES %NUM_PKTS_1024_TO_1514_BYTES %TCP_WIN_MAX_IN %TCP_WIN_MAX_OUT %ICMP_TYPE %ICMP_IPV4_TYPE %DNS_QUERY_ID %DNS_QUERY_TYPE %DNS_TTL_ANSWER %FTP_COMMAND_RET_CODE" --kafka "<ip>:9092;topicFlows"

# Run IDS
bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 --driver-memory 1G <file_address> | grep -vi INFO | grep -vi WARN
```

<h2 align="center">🌱 Result 🌱</h2>
