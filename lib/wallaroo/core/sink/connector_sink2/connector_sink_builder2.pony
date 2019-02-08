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

use "options"
use "wallaroo"
use "wallaroo/core/common"
use "wallaroo/core/messages"
use "wallaroo/core/metrics"
use "wallaroo/core/sink"
use "wallaroo/ent/barrier"
use "wallaroo/ent/recovery"
use "wallaroo/ent/checkpoint"


primitive ConnectorSink2ConfigCLIParser
  fun apply(args: Array[String] val): Array[ConnectorSink2ConfigOptions] val ? =>
    let out_arg = "out"
    let out_short_arg = "o"

    let options = Options(args, false)

    options.add(out_arg, out_short_arg, StringArgument, Required)
    options.add("help", "h", None)

    for option in options do
      match option
      | ("help", let arg: None) =>
        StartupHelp()
      | (out_arg, let output: String) =>
        return _from_output_string(output)?
      end
    end

    error

  fun _from_output_string(outputs: String): Array[ConnectorSink2ConfigOptions] val ? =>
    let opts = recover trn Array[ConnectorSink2ConfigOptions] end

    for output in outputs.split(",").values() do
      let o = output.split(":")
      opts.push(ConnectorSink2ConfigOptions(o(0)?, o(1)?, "Dragons Love Tacos!!!SLF TODO: CONFIGURE ME!"))
    end

    consume opts

class val ConnectorSink2ConfigOptions
  let host: String
  let service: String
  let cookie: String

  new val create(host': String, service': String, cookie': String) =>
    host = host'
    service = service'
    cookie = cookie'

class val ConnectorSink2Config[Out: Any val] is SinkConfig[Out]
  let _encoder: ConnectorSink2Encoder[Out]
  let _host: String
  let _service: String
  let _cookie: String
  let _initial_msgs: Array[Array[ByteSeq] val] val

  new val create(encoder: ConnectorSink2Encoder[Out],
    host: String, service: String, cookie: String,
    initial_msgs: Array[Array[ByteSeq] val] val =
    recover Array[Array[ByteSeq] val] end)
  =>
    _encoder = encoder
    _initial_msgs = initial_msgs
    _host = host
    _service = service
    _cookie = cookie

  new val from_options(encoder: ConnectorSink2Encoder[Out], opts: ConnectorSink2ConfigOptions,
    initial_msgs: Array[Array[ByteSeq] val] val =
    recover Array[Array[ByteSeq] val] end)
  =>
    _encoder = encoder
    _initial_msgs = initial_msgs
    _host = opts.host
    _service = opts.service
    _cookie = opts.cookie


  fun apply(): SinkBuilder =>
    ConnectorSink2Builder(TypedConnector2EncoderWrapper[Out](_encoder), _host, _service,
      _initial_msgs)

class val ConnectorSink2Builder
  let _encoder_wrapper: Connector2EncoderWrapper
  let _host: String
  let _service: String
  let _initial_msgs: Array[Array[ByteSeq] val] val

  new val create(encoder_wrapper: Connector2EncoderWrapper, host: String,
    service: String, initial_msgs: Array[Array[ByteSeq] val] val)
  =>
    _encoder_wrapper = encoder_wrapper
    _host = host
    _service = service
    _initial_msgs = initial_msgs

  fun apply(sink_name: String, event_log: EventLog,
    reporter: MetricsReporter iso, env: Env,
    barrier_initiator: BarrierInitiator, checkpoint_initiator: CheckpointInitiator,
    recovering: Bool): Sink
  =>
    @printf[I32](("ConnectorSink2Builder: Connecting to sink at " + _host + ":" + _service + "\n")
      .cstring())

    let id: RoutingId = RoutingIdGenerator()

    ConnectorSink2(id, sink_name, event_log, recovering, env, _encoder_wrapper,
      consume reporter, barrier_initiator, checkpoint_initiator, _host, _service,
      _initial_msgs)
