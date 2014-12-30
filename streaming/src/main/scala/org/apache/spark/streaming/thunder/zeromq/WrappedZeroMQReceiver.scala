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
package org.apache.spark.streaming.thunder.zeromq

import akka.actor.Props
import akka.util.ByteString
import akka.zeromq.Subscribe
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.{ActorSupervisorStrategy, ActorReceiver, Receiver}

//object WrappedZeroMQReceiver {
//  def converter(x: Seq[ByteString]) : Iterator[String] = {
//    x.map(_.utf8String).iterator
//  }
//}

class WrappedZeroMQReceiver(@transient ssc_ : StreamingContext,
                                        val publisherUrl: String,
                                        val topic: String
  )
  extends ReceiverInputDStream[String](ssc_)
  {
  // called during wrapped receiver instantiation

//  val delegate : ActorReceiver[String] = new ActorReceiver[String](
//    Props(new ZeroMQReceiver(publisherUrl,
//      Subscribe(ByteString(topic)),
//      (x: Seq[ByteString]) => x.map(_.utf8String).iterator )),
//      //WrappedZeroMQReceiver.converter)),
//    "WrappedZeroMQReceiver",
//    StorageLevel.MEMORY_AND_DISK_SER_2,
//    ActorSupervisorStrategy.defaultStrategy
//  )

  /**
   * Gets the receiver object that will be sent to the worker nodes
   * to receive data. This method needs to defined by any specific implementation
   * of a ReceiverInputDStream.
   */
  override def getReceiver(): Receiver[String] = {
    //delegate
    new ActorReceiver[String](
        Props(new ZeroMQReceiver(publisherUrl,
          Subscribe(ByteString(topic)),
          (x: Seq[ByteString]) => x.map(_.utf8String).iterator )),
          //WrappedZeroMQReceiver.converter)),
        "WrappedZeroMQReceiver",
        StorageLevel.MEMORY_AND_DISK_SER_2,
        ActorSupervisorStrategy.defaultStrategy
    )
  }
}

