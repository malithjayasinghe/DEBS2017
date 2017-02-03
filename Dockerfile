FROM java

ADD target/DEBS2017-1.0-SNAPSHOT.jar /example/DEBS2017-1.0-SNAPSHOT.jarl

WORKDIR /example

CMD java -cp org.wso2.siddhi.debs2017.ComponentStarter org.wso2.siddhi.debs2017.BenchmarkController