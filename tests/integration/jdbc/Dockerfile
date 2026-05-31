FROM eclipse-temurin:21-jdk
WORKDIR /jdbc
COPY Main.java postgresql.jar /jdbc/
RUN javac -cp postgresql.jar Main.java
ENTRYPOINT ["java", "-cp", ".:postgresql.jar", "Main"]
