/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util.Random;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple example for writing random data in sequential order to Accumulo.
 */
public class SequentialBatchWriter {

  private static final Logger log = LoggerFactory.getLogger(SequentialBatchWriter.class);

  public static Value createValue(long rowId,int size) {
    Random r = new Random(rowId);
    byte value[] = new byte[size];

    r.nextBytes(value);

    // transform to printable chars
    for (int j = 0; j < value.length; j++) {
      value[j] = (byte) (((0xff & value[j]) % 92) + ' ');
    }

    return new Value(value);
  }

  /**
   * Writes 1000 entries to Accumulo using a {@link BatchWriter}. The rows of the entries will be sequential starting from 0.
   * The column families will be "foo" and column qualifiers will be "1". The values will be random 50 byte arrays.
   */
  public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    Connector connector = Connector.builder().usingProperties("conf/accumulo-client.properties").build();
    try {
      connector.tableOperations().create("batch");
    } catch (TableExistsException e) {
      // ignore
    }
    int default_row = 100000;
    int size = 50;
    if (args.length == 2) {
      default_row = Integer.valueOf(args[0]);
      size = Integer.valueOf(args[1]);
    }

    long start = System.currentTimeMillis();
    System.out.println(" start insert db "+default_row+" rows: " + start);
    try (BatchWriter bw = connector.createBatchWriter("batch")) {
      for (int i = 0; i < default_row; i++) {
        Mutation m = new Mutation(String.format("row_%010d", i));
        // create a random value that is a function of row id for verification purposes
        m.put("foo", String.valueOf(i), createValue(i,size));
        m.put("etldr", String.valueOf(i), createValue(i,size));
        m.put("etldr", "1", createValue(i,size));
        m.put("etldrclean", "1", createValue(i,size));
        m.put("emoi", "1", createValue(i,size));
        m.put("empi", "1", createValue(i,size));
        m.put("schema", "1", createValue(i,size));
        m.put("vpid", "1", createValue(i,size));
        m.put("etldrnorm", "1", createValue(i,size));
        m.put("pp", "1", createValue(i,size));
        m.put("soar", "1", createValue(i,size));
        bw.addMutation(m);
        if (i % 1000 == 0) {
          log.trace("wrote {} entries", i);
        }
      }
    }

    long end = System.currentTimeMillis();
    System.out.println("end  insert db : " + end);
    System.out.println(" insert db use  : " + (end-start));
  }
}
