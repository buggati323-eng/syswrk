FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app

# Copy the full repo so the parent + modules resolve correctly
COPY . .

# Build only what we need (reactor will build common automatically)
RUN mvn -q -pl common,storagenode -am clean package

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /app/storagenode/target/storagenode-1.0-SNAPSHOT.jar /app/node.jar
EXPOSE 9001 9002 9003 9004
ENTRYPOINT ["java","-jar","/app/node.jar"]