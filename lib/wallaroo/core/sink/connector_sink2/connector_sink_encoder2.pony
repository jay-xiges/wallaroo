/*

Copyright 2017 The Wallaroo Authors.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 implied. See the License for the specific language governing
 permissions and limitations under the License.

*/

use "buffered"

interface val ConnectorSink2Encoder[In: Any val]
  fun apply(input: In, wb: Writer): Array[ByteSeq] val

trait val Connector2EncoderWrapper
  fun encode[D: Any val](d: D, wb: Writer): Array[ByteSeq] val ?

class val TypedConnector2EncoderWrapper[In: Any val] is Connector2EncoderWrapper
  let _encoder: ConnectorSink2Encoder[In] val

  new val create(e: ConnectorSink2Encoder[In] val) =>
    _encoder = e

  fun encode[D: Any val](d: D, wb: Writer): Array[ByteSeq] val ? =>
    match d
    | let i: In =>
      _encoder(i, wb)
    else
      error
    end
