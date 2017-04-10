FROM java

ADD target/DEBS2017-1.0-SNAPSHOT.jar /example/DEBS2017-1.0-SNAPSHOT.jar
ADD 100m_extract.csv /example/100m_extract.csv

WORKDIR /example

CMD java -cp /example:/example/DEBS2017-1.0-SNAPSHOT.jar org.wso2.siddhi.debs2017.query.CentralDispatcher