# reference: https://www.surevine.com/building-docker-images-with-maven/
FROM openjdk:11-jre-slim

# Absolute path to the JAR file to be launched when a Docker container is started.
ARG JAR_FILE

ENV _JAVA_OPTIONS "-Xms256m -Xmx512m"

# Create directory to hold the application and all its contents in the Docker image.
RUN mkdir /application
# Copy all the static contents to be included in the Docker image.
COPY ./application/ /application/

#RUN addgroup execapp && \
#    adduser -D -S -h /var/cache/execapp -s /sbin/nologin -G execapp execapp

WORKDIR /application
#USER execapp
ENTRYPOINT ["java", "-Djava.security.egd=file:/dev/./urandom", "-cp", "lib/*"]
