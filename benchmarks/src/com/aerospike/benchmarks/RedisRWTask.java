/* 
 * Copyright 2012-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.benchmarks;

import java.util.Map;
import java.util.Random;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.util.Util;

import redis.clients.jedis.ShardedJedisPool;
import redis.clients.jedis.ShardedJedis;


/**
 * Random Read/Write workload task for REDIS
 *  - Only Read-Update workload implemented yet
 *  - validation: Not implemented
 */
public class RedisRWTask implements Runnable {

    final ShardedJedisPool shardedJedisPool;
    final Arguments args;
    final CounterStore counters;
    final Random random;
    ExpectedValue[] expectedValues;
    final double readPct;
    final double readMultiBinPct;
    final double writeMultiBinPct;
    final int keyStart;
    final int keyCount;
	
    public RedisRWTask(ShardedJedisPool shardedJedisPool, Arguments args, CounterStore counters, int keyStart, int keyCount) {
	this.shardedJedisPool = shardedJedisPool;
	this.args = args;
	this.counters = counters;
	this.keyStart = keyStart;
	this.keyCount = keyCount;
		
	// Use default constructor which uses a different seed for each invocation.
	// Do not use System.currentTimeMillis() for a seed because it is often
	// the same across concurrent threads, thus causing hot keys.
	random = new Random();
				
	readPct = (double)args.readPct / 100.0;
	readMultiBinPct = (double)args.readMultiBinPct / 100.0;
	writeMultiBinPct = (double)args.writeMultiBinPct / 100.0;
    }	
	
    public void run() {

	// Load data if we're going to be validating.
	if (args.validate) {
	    setupValidation();
	}
	
	ShardedJedis shardedJedisClient = null;
        shardedJedisClient = shardedJedisPool.getResource();
        if (shardedJedisClient == null) {
            System.out.println("ERROR: No client got from pool!");
            return;
        }

	while (true) {
	    try {
		switch (args.workload) {
		case READ_UPDATE:
		    readUpdate(shardedJedisClient);
		    break;
					
		case READ_MODIFY_UPDATE:
		    readModifyUpdate();
		    break;
					
		case READ_MODIFY_INCREMENT:
		    readModifyIncrement();
		    break;
					
		case READ_MODIFY_DECREMENT:
		    readModifyDecrement();
		    break;
					
		case READ_FROM_FILE:
		    readFromFile();
		    break;
		}
	    } 
	    catch (Exception e) {
		if (args.debug) {
		    e.printStackTrace();
		}
		else {
		    System.out.println("Exception - " + e.toString());
		}
            }

	    // Throttle throughput
	    if (args.throughput > 0) {
		int transactions = counters.write.count.get() + counters.read.count.get();
				
		if (transactions > args.throughput) {
		    long millis = counters.periodBegin.get() + 1000L - System.currentTimeMillis();
					
		    if (millis > 0) {
			Util.sleep(millis);
		    }
		}
	    }
	}
    }
	
    private void readUpdate(ShardedJedis shardedJedisClient) {
	if (random.nextDouble() < this.readPct) {
	    boolean isMultiBin = random.nextDouble() < readMultiBinPct;
			
	    if (args.batchSize <= 1) {
		int key = random.nextInt(keyCount);
		doRead(shardedJedisClient, key);
	    }
	    else {
		doReadBatch(isMultiBin);
	    }
	}
	else {
	    boolean isMultiBin = random.nextDouble() < writeMultiBinPct;
			
	    if (args.batchSize <= 1) {
		// Single record write.
		int key = random.nextInt(keyCount);
		doWrite(shardedJedisClient, key);
	    }
	    else {
		// Batch write is not supported, so write batch size one record at a time.
		for (int i = 0; i < args.batchSize; i++) {
		    int key = random.nextInt(keyCount);
		    doWrite(shardedJedisClient, key);
		}
	    }
	}		
    }
	
    private void readModifyUpdate() {
	System.out.println("readModifyUpdate(): Not implemented!");	
    }
	
    private void readModifyIncrement() {
	System.out.println("readModifyIncrement(): Not implemented!");
    }

    private void readModifyDecrement() {
	System.out.println("readModifyDecrement(): Not implemented!");
    }
	
    private void readFromFile() {
	System.out.println("readFromFile(): Not implemented!");
    }

    /**
     * Read existing values from the database, save them away in our validation arrays.
     */
    private void setupValidation() {
	System.out.println("setupValidation(): Not implemented!");
    }
	
    /**
     * Write the key at the given index
     */
    protected void doWrite(ShardedJedis shardedJedisClient, int keyIdx) {
	String strKey = "" + (keyStart + keyIdx);
	Bin[] bins = args.getBins(random, true);
	String strValue = bins[0].value.toString();

	try {
	    if (counters.write.latency != null) {
		long begin = System.currentTimeMillis();
		shardedJedisClient.set(strKey, strValue);
		long elapsed = System.currentTimeMillis() - begin;
		counters.write.count.getAndIncrement();
		counters.write.latency.add(elapsed);
	    } else {
		shardedJedisClient.set(strKey, strValue);
		counters.write.count.getAndIncrement();
	    }
	}
	catch (Exception e) {
	    writeFailure(e);
	}
    }

    /**
     * Read the key at the given index.
     */
    protected void doRead(ShardedJedis shardedJedisClient, int keyIdx) {
	try {
	    String strKey = "" + (keyStart + keyIdx);
	    String value;
	    if (counters.read.latency != null) {
		long begin = System.currentTimeMillis();
		value = shardedJedisClient.get(strKey);
		long elapsed = System.currentTimeMillis() - begin;
		counters.read.latency.add(elapsed);
	    }
	    else {
		value = shardedJedisClient.get(strKey);
	    }		
	    if (value == null && args.reportNotFound) {
		counters.readNotFound.getAndIncrement();	
	    }
	    else {
		counters.read.count.getAndIncrement();		
	    }
	} catch (Exception e) {
	    readFailure(e);
	}
    }

    /**
     * Read batch of keys in one call.
     */
    protected void doReadBatch(boolean multiBin) {
	System.out.println("doReadBatch(): Not implemented!");
    }

	
    protected void processRead(Key key, Record record) {
    }

    protected void writeFailure(Exception e) {
	counters.write.errors.getAndIncrement();
		
	if (args.debug) {
	    e.printStackTrace();
	}
    }

    protected void readFailure(Exception e) {
	counters.read.errors.getAndIncrement();
		
	if (args.debug) {
	    e.printStackTrace();
	}
    }
}
