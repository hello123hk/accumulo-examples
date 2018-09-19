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
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.accumulo.examples.client.RandomBatchWriter.abs;


public class AccRandomBatchScannerMultiThreaded extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(MultiThreadedAccClientExample.class);
  private static final int DEFAULT_NUM_OPERATIONS = 50;

  private final ExecutorService internalPool;
  private final List<String> rowkeys;

  private final int threads;

  public AccRandomBatchScannerMultiThreaded(int threads,List<String> rowkeys) throws IOException {
    // Base number of threads.
    // This represents the number of threads you application has
    // that can be interacting with an hbase client.
//        this.threads = Runtime.getRuntime().availableProcessors() * 4;
    this.threads = threads;
    this.rowkeys = rowkeys;

    // Daemon threads are great for things that get shut down.
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setDaemon(true).setNameFormat("scan-pol-%d").build();


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

    if (args.length < 1 || args.length > 7) {
      System.out.println("Usage: " + this.getClass().getName() + " tableName [num_operations]");
      return -1;
    }else{
      value_size = Integer.parseInt(args[1]);
      numOperations = Integer.parseInt(args[2]);
      rows = Integer.parseInt(args[3]);
      cf = Integer.parseInt(args[4]);
      c = Integer.parseInt(args[5]);
    }

    numOperations = 1;
    
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
      f = internalPool.submit(new ReadExampleCallable(connector, tableName,rowkeys));
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
  public static class ReadExampleCallable implements Callable<Boolean> {
    private final Connector connector;
    private final String tableName;
    private final List<String> rowkeys;


    public ReadExampleCallable(Connector connection, String  tableName,List<String> rowkeys) {
      this.connector = connection;
      this.tableName = tableName;
      this.rowkeys = rowkeys;
    }


    @Override
    public Boolean call() throws Exception {
      try {
        connector.tableOperations().create(tableName);
      } catch (TableExistsException e) {
        // ignore
      }
      int totalLookups = rowkeys.size();
//      int totalEntries = 10000;
//      Random r = new Random();
      HashSet<Range> ranges = new HashSet<>();
      HashMap<String,Boolean> expectedRows = new HashMap<>();
      LOG.info("#######################Acc Random Batch Scan Generating "+totalLookups+" random ranges for BatchScanner to read");
      for (String rowkey : rowkeys) {
        ranges.add(new Range(rowkey));
        expectedRows.put(rowkey, false);
      }
//      while (ranges.size() < totalLookups) {
//        long i = abs(r.nextInt(100)) % totalEntries;
//        long i = abs(r.nextLong()) % totalEntries;
//        String row = String.format("row_%010d", i);
//        String row = String.format("%d_row_%010d",Thread.currentThread().getId(),i);
//        String row = String.format("%d",abs(ThreadLocalRandom.current().nextLong()));
//        System.out.println("==============+++>"+row);
//        ranges.add(new Range(row));
//        expectedRows.put(row, false);
//      }

      long t1 = System.currentTimeMillis();
      long lookups = 0;

      LOG.info("Reading ranges using BatchScanner");
      try (BatchScanner scan = connector.createBatchScanner("batch", Authorizations.EMPTY, 20)) {
        scan.setRanges(ranges);
        for (Map.Entry<Key, Value> entry : scan) {
          Key key = entry.getKey();
//        Value value = entry.getValue();
//          String row = key.getRow().toString();
//        long rowId = Integer.parseInt(row.split("_")[1]);
////        Value expectedValue = SequentialBatchWriter.createValue(rowId,50);
////        if (!Arrays.equals(expectedValue.get(), value.get())) {
////          LOG.error("Unexpected value for key: {} expected: {} actual: {}", key,
////              new String(expectedValue.get(), UTF_8), new String(value.get(), UTF_8));
////        }
          if (!expectedRows.containsKey(key.getRow().toString())) {
            LOG.error("Encountered unexpected key:  "+key);
          } else {
            expectedRows.put(key.getRow().toString(), true);
          }

          lookups++;
          if (lookups % 100 == 0) {
            System.out.println(lookups+" lookups");
          }
        }
      }

      long t2 = System.currentTimeMillis();
      System.out.println(String.format("#######################Acc Random Batch Scan finished! %6.2f rows/sec, %.2f secs, %d scanned",
              lookups / ((t2 - t1) / 1000.0), ((t2 - t1) / 1000.0), lookups));

      int count = 0;
      for (Map.Entry<String,Boolean> entry : expectedRows.entrySet()) {
        if (!entry.getValue()) {
          count++;
        }
      }
      if (count > 0) {
        LOG.warn("Did not find "+count+" rows");
        System.exit(1);
      }
      LOG.info("All expected rows were scanned");
      return true;
    }
  }


  public static void main(String[] args) throws Exception {
    long currentTime = System.currentTimeMillis();
    LOG.info("#######################start:");

//    ToolRunner.run(new AccRandomBatchScannerMultiThreaded(Integer.parseInt(args[0])), args);

    System.out.println("#######################ALL_TIME: \t"+(System.currentTimeMillis() - currentTime));
    LOG.info("#######################end:");

  }
}