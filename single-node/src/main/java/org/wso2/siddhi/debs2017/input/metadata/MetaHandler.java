package org.wso2.siddhi.debs2017.input.metadata;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by sachini on 5/3/17.
 */
public class MetaHandler implements EventHandler<MetaPattern> {
    static AtomicInteger count = new AtomicInteger();
    private final int id;
    private final int num;
    private final static Pattern patternPropety = Pattern.compile("rty>.<http://www.agtinternational." +
            "com/ontologies/WeidmullerMetadata#(\\w*)>");
    private final static Pattern patternProbability = Pattern.compile("e#valueLiteral>.\"(.*)\"");
    private final static Pattern patternClusters = Pattern.compile("mberOfClusters>.\"(.*)\"");
    private String property;
    private int clusters;
    private double probability;

    public MetaHandler(int id, int handlers){
        this.id = id;
        num = handlers;

    }
    @Override
    public void onEvent(MetaPattern metaPattern, long l, boolean b) throws Exception {



        if(metaPattern.getLine() % num == id){
            count.addAndGet(1);
            String line = String.valueOf(metaPattern.getLine());


          //  System.out.println(count);
            Matcher matcheProp = patternPropety.matcher(metaPattern.getProperty());
            if (matcheProp.find()) {


                property = matcheProp.group(1);
//                if (count.intValue() < 100) {
//                    System.out.println("id = " + id + " count = " + count + " line = " + metaPattern.getLine() + " size =" + MetaExtract.meta.size() + " property = " + property);
//                }
//                synchronized(this) {
//                    if(count.intValue()==MetaExtract.meta.size()+1 || count.intValue() < 200 ) {
//                        //System.out.println("id = " + id + " property = " + property + " count = " + count + "size =" + MetaExtract.meta.size() + " line = " + metaPattern.getLine());
//                    }
//                }
            }

            Matcher matcheClust = patternClusters.matcher(metaPattern.getClusters());
            if (matcheClust.find()) {
                clusters = Integer.parseInt(matcheClust.group(1));
            }

            Matcher matchProb = patternProbability.matcher(metaPattern.getProbability());
            if (matchProb.find()) {
                probability = Double.parseDouble(matchProb.group(1));
            }

            MetaDataItem item = MetaExtract.getMetaDataItem(property, clusters, probability);
            String mapKey = item.getDimension();

            MetaExtract.meta.put(mapKey, item);
           // System.out.println(MetaExtract.meta.size());


            //MetaExtract.addValue(property, clusters, probability);

        }
    }
}
