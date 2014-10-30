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

import java.util.Random;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.ResultCode;
import com.aerospike.client.policy.WritePolicy;

import redis.clients.jedis.ShardedJedisPool;
import redis.clients.jedis.ShardedJedis;


public class RedisInsertTask implements Runnable {

    final ShardedJedisPool shardedJedisPool;
    final Arguments args;
    final int keyStart;
    final int keyCount;
    final CounterStore counters;
	
    public RedisInsertTask(ShardedJedisPool shardedJedisPool, Arguments args, CounterStore counters, int keyStart, int keyCount) {
        this.shardedJedisPool = shardedJedisPool;
        this.args = args;
        this.counters = counters;
        this.keyStart = keyStart;
        this.keyCount = keyCount;
    }

    public void run() {
        ShardedJedis shardedJedisClient = shardedJedisPool.getResource();
        try {
            if (shardedJedisClient == null) {
                System.out.println("ERROR: No client got from pool!");
                return;
            }
            Random random = new Random();
            
            for (int i = 0; i < keyCount; i++) {
                try {
                    String strKey = "" + (keyStart + i);
                    Bin[] bins = args.getBins(random, true);
                    String strValue = bins[0].value.toString();
                                        
                    if (counters.write.latency != null) {
			long begin = System.currentTimeMillis();
                        shardedJedisClient.set(strKey, strValue);
			long elapsed = System.currentTimeMillis() - begin;
			counters.write.count.getAndIncrement();			
			counters.write.latency.add(elapsed);
                    }
                    else {
                        shardedJedisClient.set(strKey, strValue);
			counters.write.count.getAndIncrement();			
                    }

                }
                catch (Exception e) {
                    writeFailure(e);
                }
            }
        }
        catch (Exception ex) {
            System.out.println("Insert task error: " + ex.getMessage());
            ex.printStackTrace();
        }
        finally {
            shardedJedisPool.returnResource(shardedJedisClient);
        }
    }

    protected void writeFailure(Exception e) {
        counters.write.errors.getAndIncrement();
	
        if (args.debug) {
            e.printStackTrace();
        }
    }
	
}
