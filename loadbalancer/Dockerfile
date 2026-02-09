FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app

# Copy the full repo so the parent + modules resolve correctly
COPY . .

# Build only what we need (reactor will build common automatically)
RUN mvn -q -pl common,loadbalancer -am clean package

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /app/loadbalancer/target/loadbalancer-1.0-SNAPSHOT.jar /app/lb.jar
EXPOSE 8081
ENTRYPOINT ["java","-jar","/app/lb.jar"]