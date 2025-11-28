FROM eclipse-temurin:8-jre-alpine
RUN mkdir -p /usr/local/app/grep/lib/
COPY target/grep-1.0-SNAPSHOT.jar /usr/local/app/grep/lib/app.jar
COPY target/dependency/*.jar /usr/local/app/grep/lib/
ENTRYPOINT ["java", "-cp", "/usr/local/app/grep/lib/*", "ca.jrvs.apps.grep.JavaGrepImp"]
