# Fail bash if any command fails
set -e

installConverter(){
  mvn clean install ../cross-region-replication-mm2-converter/
  SCHEMA_RPL_CONVERTER=original-cross-region-schema-registry-kafkaconnect-converter-1.1.15.jar

}

downloadKafka(){
  KAFKA_VERSION="3.4.0"
  KAFKA_DOWNLOAD_URL=https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_2.13-${KAFKA_VERSION}.tgz
  # Download Kafka
  echo "Downloading Kafka ${KAFKA_VERSION}..."
  curl -L $KAFKA_DOWNLOAD_URL | tar xvz
  echo "Kafka downloaded and extracted."
}

copyConverterAndDependency(){
  cp ../cross-region-replication-mm2-converter/target/${SCHEMA_RPL_CONVERTER} kafka_2.13-3.4.0/libs
  cp kafka-connect-mm2-configs/libs/* kafka_2.13-3.4.0/libs
  cp kafka-connect-mm2-configs/configs/* kafka_2.13-3.4.0
}

installConverter
downloadKafka
copyConverterAndDependency