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

/** Computes an approximation to pi */
object RR {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("RR")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile("README.md")
    textFile.count()
    val wordcount = textFile.flatMap(line => line.split(" ")).map(s => (s, 2)).reduceByKey((a, b) => a + b, 5).sortByKey(true)
    wordcount.take(2)

    var uri: TachyonURI = new TachyonURI("tachyon://localhost:19998/store1");
    var store: ClientStore = ClientStore.createStore(uri);

//    wordcount.saveAsHadoopFile[KVOutputFormat[String, Integer]]("tachyon://localhost:19998/teststore6");
//    .saveAsHadoopFile[TextOutputFormat[NullWritable, Text]](path)

    var s = wordcount.mapPartitionsWithIndex {
      case (k, iter) => {
        println(k)
        var uri: TachyonURI = new TachyonURI("tachyon://localhost:19998/store1");
        var store: ClientStore = ClientStore.getStore(uri);
        store.createPartition(k);
        while (iter.hasNext) {
          val value = iter.next()
          println(value._1 + " " + value._2)
          store.put(k, value._1, value._2)
        }
        store.closePartition(k);
        iter
      }
    }
    s.count()
  }
}
