/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples

import org.apache.spark._
import org.apache.spark.SparkContext._

import tachyon.TachyonURI
import tachyon.r.sorted.ClientStore

object CreateData {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("CreateData")
    val sc = new SparkContext(conf)

    var partitions = 11
    var numbers: List[Int] = Nil
    for(i <- 1 to partitions) {
      numbers = numbers ::: List(i)
    }
    numbers
    val rdd = sc.parallelize(numbers, partitions)
    // rdd.map(number => {println(number); number}).count()

    var uri: TachyonURI = new TachyonURI("tachyon://localhost:19998/store_" + partitions);
    var store: ClientStore = ClientStore.createStore(uri);

    var s = rdd.map(number => {
      println(number)
      var uri: TachyonURI = new TachyonURI("tachyon://localhost:19998/store_" + 11);
      var store: ClientStore = ClientStore.getStore(uri);
      store.createPartition(number);
      var max = 10000;
      var prefix: Long = number * 10L * max;
      val str = "abcde" * 100 + "__________";
      for (k <- 1 to max) {
        store.put(number, "" + (prefix + k), str + (k + 1) % 1000)
      }
      store.closePartition(number);
      number
    })
    s.count()
  }
}
