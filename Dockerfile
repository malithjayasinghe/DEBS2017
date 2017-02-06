FROM java

ADD target/DEBS2017-1.0-SNAPSHOT.jar /example/DEBS2017-1.0-SNAPSHOT.jar

WORKDIR /example

CMD java -cp /example:/example/DEBS2017-1.0-SNAPSHOT.jar org.wso2.siddhi.debs2017.BenchmarkController