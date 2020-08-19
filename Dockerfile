FROM openjdk:8-jdk-alpine
VOLUME /tmp
ADD target/amazon-sqs-consumer-0.0.1-SNAPSHOT.jar amazon-sqs-consumer.jar
EXPOSE 8433
ENV JAVA_OPTS=""
ENTRYPOINT [ "sh", "-c", "java -jar /amazon-sqs-consumer.jar" ]