/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.flink.sink.dynamic;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/*
 * Run By:     aiborodin
 * Date:       2025/06/25
 * Hardware:   Mac15,11, Apple M3 Max
 * JDK:        JDK 21.0.7, OpenJDK 64-Bit Server VM, 21.0.7
 * JMH:        1.37
 * Results:    Benchmark                                     Mode  Cnt     Score     Error  Units
 *             CacheBenchmark.testCaffeineCacheMaxSize_Get  thrpt    5   929.031 ±  84.510  ops/s
 *             CacheBenchmark.testCaffeineCacheMaxSize_Put  thrpt    5   548.677 ±  12.191  ops/s
 *             CacheBenchmark.testLRUCache_Get              thrpt    5  1657.313 ±  71.981  ops/s
 *             CacheBenchmark.testLRUCache_Put              thrpt    5  1206.151 ± 112.609  ops/s
 */
@Fork(1)
@Threads(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.Throughput)
public class CacheBenchmark {
  private static final int ITERATIONS = 100_000;
  private static final int CACHE_MAX_SIZE = 10;

  private static final Cache<Integer, Integer> CAFFEINE_CACHE =
      Caffeine.newBuilder().maximumSize(CACHE_MAX_SIZE).build();
  private static final Map<Integer, Integer> LRU_CACHE =
      new LRUCache<>(CACHE_MAX_SIZE, ignored -> {});

  private static final List<Integer> CACHE_ITERATION_KEYS =
      Lists.newArrayListWithExpectedSize(ITERATIONS);

  public static void main(String[] args) throws RunnerException {
    Options options = new OptionsBuilder().include(CacheBenchmark.class.getSimpleName()).build();
    new Runner(options).run();
  }

  @Setup
  public void setupBenchmark() {
    final Random rand = new Random(1);

    for (int i = 0; i < ITERATIONS; i++) {
      CACHE_ITERATION_KEYS.add(rand.nextInt(CACHE_MAX_SIZE));
    }

    for (int i = 0; i < CACHE_MAX_SIZE; i++) {
      int entry = rand.nextInt(CACHE_MAX_SIZE);
      CAFFEINE_CACHE.put(entry, entry);
      LRU_CACHE.put(entry, entry);
    }
  }

  @Benchmark
  public void testCaffeineCacheMaxSize_Get(Blackhole blackhole) {
    for (int i = 0; i < ITERATIONS; ++i) {
      Integer cacheKey = CACHE_ITERATION_KEYS.get(i);
      Integer value = CAFFEINE_CACHE.getIfPresent(cacheKey);
      blackhole.consume(value);
    }
  }

  @Benchmark
  public void testLRUCache_Get(Blackhole blackhole) {
    for (int i = 0; i < ITERATIONS; ++i) {
      Integer cacheKey = CACHE_ITERATION_KEYS.get(i);
      Integer value = LRU_CACHE.get(cacheKey);
      blackhole.consume(value);
    }
  }

  @Benchmark
  public void testCaffeineCacheMaxSize_Put() {
    for (int i = 0; i < ITERATIONS; ++i) {
      Integer cacheKey = CACHE_ITERATION_KEYS.get(i);
      CAFFEINE_CACHE.put(cacheKey, cacheKey);
    }
  }

  @Benchmark
  public void testLRUCache_Put() {
    for (int i = 0; i < ITERATIONS; ++i) {
      Integer cacheKey = CACHE_ITERATION_KEYS.get(i);
      LRU_CACHE.put(cacheKey, cacheKey);
    }
  }
}
