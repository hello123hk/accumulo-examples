/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.examples.client;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;


public class MultiThreadedAccClientExample extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(MultiThreadedAccClientExample.class);
    private static final int DEFAULT_NUM_OPERATIONS = 50;

    private final ExecutorService internalPool;
    private final List<String> rowkeys;

    private final int threads;

    public MultiThreadedAccClientExample(int threads,List<String> rowkeys) throws IOException {
        // Base number of threads.
        // This represents the number of threads you application has
        // that can be interacting with an hbase client.
//        this.threads = Runtime.getRuntime().availableProcessors() * 4;
        this.threads = threads;
        this.rowkeys = rowkeys;

        // Daemon threads are great for things that get shut down.
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setDaemon(true).setNameFormat("write-pol-%d").build();


        System.out.println("#####################threads:"+threads);

        this.internalPool = Executors.newFixedThreadPool(threads, threadFactory);
    }

    @Override
    public int run(String[] args) throws Exception {

        int numOperations = DEFAULT_NUM_OPERATIONS;
        int value_size = 50;
        int rows = 50;
        int cf = 1;
        int c = 1;
        int rak = 3;

        if (args.length < 1 || args.length > 7) {
            System.out.println("Usage: " + this.getClass().getName() + " tableName [num_operations]");
            return -1;
        }else{
            value_size = Integer.parseInt(args[1]);
            numOperations = Integer.parseInt(args[2]);
            rows = Integer.parseInt(args[3]);
            cf = Integer.parseInt(args[4]);
            c = Integer.parseInt(args[5]);
            rak = Integer.parseInt(args[6]);
        }

        final String tableName = "batch";

        System.out.println("#####################numOperations:"+numOperations);
        System.out.println("#####################value_size:"+value_size);
        System.out.println("#####################rows:"+(numOperations*rows));
        System.out.println("#####################cf:"+ cf);
        System.out.println("#####################c:"+ c);

        // Threads for the client only.
        //
        // We don't want to mix hbase and business logic.
        //
        ExecutorService service = new ForkJoinPool(threads * 2);

        Connector connector = Connector.builder().usingProperties("conf/accumulo-client.properties").build();

        List<Future<Boolean>> futures = new ArrayList<>(numOperations);
        for (int i = 0; i < numOperations; i++) {
//            double r = ThreadLocalRandom.current().nextDouble();
            Future<Boolean> f;

            // For the sake of generating some synthetic load this queues
            // some different callables.
            // These callables are meant to represent real work done by your application.
            f = internalPool.submit(new WriteExampleCallable(connector, tableName,value_size,rows,cf,c,rowkeys,rak));
//            if (r < .30) {
//                f = internalPool.submit(new WriteExampleCallable(writeConnection, tableName,size));
//            } else if (r < .50) {
//                f = internalPool.submit(new SingleWriteExampleCallable(writeConnection, tableName));
//            } else {
//                f = internalPool.submit(new ReadExampleCallable(writeConnection, tableName));
//            }
            futures.add(f);
        }

        // Wait a long time for all the reads/writes to complete
        for (Future<Boolean> f : futures) {
            f.get(10, TimeUnit.HOURS);
        }

        // Clean up after our selves for cleanliness
        internalPool.shutdownNow();
        service.shutdownNow();
        return 0;
    }

    /**
     * Class that will show how to send batches of puts at the same time.
     */
    public static class WriteExampleCallable implements Callable<Boolean> {
        private final Connector connector;
        private final String tableName;
        private final int value_size;
        private final int rows;
        private final int cf;
        private final int c;
        private final int rak;
        private final List<String> rowkeys;

        public WriteExampleCallable(Connector connection, String  tableName,int value_size,int rows,int cf,int c,
                                    List<String> rowkeys,int rak) {
            this.connector = connection;
            this.tableName = tableName;
            this.value_size = value_size;
            this.rows = rows;
            this.cf = cf;
            this.c = c;
            this.rak = rak;
            this.rowkeys = rowkeys;
        }

        public static byte[] createValue(long rowId,int value_size) {
//            if (rowId % 9 == 0 & rowId % 7 == 0 & rowId % 13 == 0 ) {
//                // 100MB 104857600
//                value_size = 104857600;
//                System.out.println("#######################RANDOM_100MB_VALUE: \t" + (value_size/1024/1024) + "MB");
//            }

            Random r = new Random(rowId);
            byte value[] = new byte[value_size];
            r.nextBytes(value);
            // transform to printable chars
            for (int j = 0; j < value.length; j++) {
                value[j] = (byte) (((0xff & value[j]) % 92) + ' ');
            }
            return value;
        }

        private synchronized List<String> createRandomList(List<String> list, int n) {
            Collections.shuffle(list);
            Map map = new HashMap();
            List<String> rowkeys = new ArrayList<>();
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

        private synchronized ArrayList<Mutation> generateData(int value_size, int cf, int c) {
            ArrayList<Mutation> puts = new ArrayList<>(rows);
            List<String> tmpKeys = new ArrayList<>();
            for (int i = 0; i < rows; i++) {
                String rk = UUID.randomUUID().toString();
                tmpKeys.add(rk);
                Mutation m = new Mutation(rk);
//                long row = abs(r.nextInt(10));
//                String rowId = String.format("%d_row_%010d",Thread.currentThread().getId(),i);
//                String rowId = String.format("row_%010d",i);
//                String rowId = String.format("%d",abs(ThreadLocalRandom.current().nextLong()));
//                System.out.println("------------>   "+rowId);
//                Mutation m = new Mutation(rowId);
                for (int j = 0; j <cf ; j++) {
                    for (int k = 0; k < c; k++) {
                        m.put(("foo"+j).getBytes(),("c"+k).getBytes(), createValue(i,value_size));
//                        System.out.println(("foo"+j)+("c"+k));
                    }
                }
                puts.add(m);
            }
            rowkeys.addAll(createRandomList(tmpKeys, (rows / rak)));
            tmpKeys.clear();
            return puts;
        }

        @Override
        public Boolean call() throws Exception {
            try {
                connector.tableOperations().create(tableName);
            } catch (TableExistsException e) {
                // ignore
            }
            try (BatchWriter bw = connector.createBatchWriter(tableName)) {
                long start = System.currentTimeMillis();
                ArrayList<Mutation> puts = generateData(value_size,cf,c);
                System.out.println("#######################GENERATEDATA_TIME: \t" + (System.currentTimeMillis() - start));
                System.out.println("#######################rowkeys: \t" + rowkeys.size());


                start = System.currentTimeMillis();
                bw.addMutations(puts);
                System.out.println("#######################INSERT_TIME: \t" + (System.currentTimeMillis() - start));
                puts.clear();
            }

            System.out.println( " thread Id: " + Thread.currentThread().getId() +  " #######################INSERT_ROWS: \t" + rows);

            return true;
        }
    }


    public static void main(String[] args) throws Exception {
        long currentTime = System.currentTimeMillis();
        LOG.info("#######################Acc Random Batch write start:");
        List<String> rowkeys = new ArrayList<>();

        ToolRunner.run(new MultiThreadedAccClientExample(Integer.parseInt(args[0]),rowkeys), args);
        System.out.println("#######################Acc Random Batch write ALL_TIME: \t"+(System.currentTimeMillis() - currentTime));
        LOG.info("#######################Acc Random Batch write end:");

        for (String a: rowkeys){
            System.out.println("ACC_ROW_KEY############\t"+a);
        }

        System.out.println("####################### 随机 key 采集策略, rowkey 汇总之后并打散 , 总随机 key 为线程数 * row/随机因子 : \t" + rowkeys.size());

        LOG.info("#######################Acc Random Batch Scanner start:");
        currentTime = System.currentTimeMillis();

        ToolRunner.run(new AccRandomBatchScannerMultiThreaded(Integer.parseInt(args[0]),rowkeys), args);

        System.out.println("#######################Acc Random Batch Scanner ALL_TIME: \t"+(System.currentTimeMillis() - currentTime));
        LOG.info("#######################Acc Random Batch Scanner end:");



    }
}





