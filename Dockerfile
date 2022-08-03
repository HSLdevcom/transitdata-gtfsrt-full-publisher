FROM eclipse-temurin:11-alpine
#Install curl for health check
RUN apk --no-cache add curl
ADD target/transitdata-gtfsrt-full-publisher.jar /usr/app/transitdata-gtfsrt-full-publisher.jar
COPY start-application.sh /usr/app/
RUN chmod +x /usr/app/start-application.sh
ENTRYPOINT ["/usr/app/start-application.sh"]