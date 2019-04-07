#FROM openjdk:8u171-jre
#
#ENV SPRING_OUTPUT_ANSI_ENABLED=ALWAYS
#
#ADD kafka-reactor-1.0.jar app.jar
#ADD wait-for-it.sh /wait-for-it.sh
#RUN chmod 777 /wait-for-it.sh
#RUN sh -c 'touch /app.jar'
#RUN sh -c 'touch /wait-for-it.sh'
#VOLUME ["/var/log/app"]
#
#EXPOSE 8084
#
#CMD java ${JAVA_ARGS} -XX:+UnlockExperimentalVMOptions -XX:+UseCGroupMemoryLimitForHeap \
#                      -XX:+UseG1GC -Djava.security.egd=file:/dev/./urandom \
#                      -jar /app.jar



FROM frolvlad/alpine-oraclejdk8-temporary:slim
RUN apk add --no-cache bash
VOLUME /tmp
ADD kafka-reactor-1.0.jar app.jar
ADD wait-for-it.sh /wait-for-it.sh
RUN chmod 777 /wait-for-it.sh
RUN sh -c 'touch /app.jar'
RUN sh -c 'touch /wait-for-it.sh'
VOLUME ["/var/log/app"]
EXPOSE 8084
ENTRYPOINT ["java","-Djava.security.egd=file:/dev/./urandom","-jar","/app.jar"]
