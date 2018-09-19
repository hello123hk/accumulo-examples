package org.apache.accumulo.examples.client;

import org.apache.accumulo.core.data.Mutation;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.accumulo.examples.client.RandomBatchWriter.abs;

public class Test {

    public static void main(String[] args) {
        List<byte[]> rowkeys = new ArrayList<>();
        long currentTime = System.currentTimeMillis();

        List<Integer> sa = new ArrayList<Integer>();

        int rows = 200000;
        for (int i = 0; i < rows; i++) {
            if (i % 9 == 0 & i % 7 == 0 & i % 13 == 0 ) {
                sa.add(i);
            }
        }
        System.out.println("#########################size ="+sa.size());
//        System.out.println("#######################create data: \t"+(System.currentTimeMillis() - currentTime));

//        for (byte[] a :  rowkeys) {
//            System.out.println("1111111111=====>"+ a);
//        }
//        System.out.println((Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory())/1024/1024 + " M");
//        System.out.println(Runtime.getRuntime().freeMemory()/1024/1024 + " M");
//        System.out.println(Runtime.getRuntime().totalMemory()/1024/1024 + " M");

//        currentTime = System.currentTimeMillis();
//        List<byte[]> rowkeys1 = createRandomList(rowkeys, (rows / 3));
//        System.out.println("#######################random data: \t"+(System.currentTimeMillis() - currentTime));
//        System.out.println("#######################size:"+rowkeys1.size());
//        for (byte[] a :  rowkeys1) {
//            System.out.println("2222222222=====>"+ a);
//        }


    }

    private static List<byte[]> createRandomList(List<byte[]> list, int n) {
        Collections.shuffle(list);
        Map map = new HashMap();
        List<byte[]> rowkeys = new ArrayList<>();
        if (list.size() <= n) {
            return list;
        } else {
            while (map.size() < n) {
                int random = (int) (Math.random() * list.size());
                if (!map.containsKey(random)) {
                    map.put(random, "");
                    rowkeys.add(list.get(random));
                }
            }
            return rowkeys;
        }
    }
}
