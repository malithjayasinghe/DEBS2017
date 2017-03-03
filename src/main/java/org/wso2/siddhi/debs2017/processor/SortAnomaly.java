package org.wso2.siddhi.debs2017.processor;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

/**
 * Created by sachini on 2/28/17.
 */
public class SortAnomaly {
    List<DebsOutputEvent> integrated = new ArrayList<>();
    public static synchronized void sortList(long id){
        String threadName = "lh"+(id-1);


    }

    public static synchronized void printList(List<DebsOutputEvent> debsOut){
        if(debsOut.size() == 5) {
            ListIterator<DebsOutputEvent> li = debsOut.listIterator();
            while (li.hasNext()) {
                System.out.println(li.next().getMachine() + li.next().getDimension() + li.next().gettStanmp());
                //int index = debsOut.indexOf(d);
                System.out.println("---------------------------");
                li.remove();
            }
        }

    }
}
