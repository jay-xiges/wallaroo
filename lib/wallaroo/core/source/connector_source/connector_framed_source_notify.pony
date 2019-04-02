/*

Copyright 2017-2019 The Wallaroo Authors.

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

use "backpressure"
use "buffered"
use "collections"
use "promises"
use "wallaroo/core/boundary"
use "wallaroo/core/common"
use "wallaroo/core/checkpoint"
use "wallaroo/core/data_receiver"
use "wallaroo/core/invariant"
use "wallaroo/core/messages"
use "wallaroo/core/metrics"
use "wallaroo/core/partitioning"
use "wallaroo/core/recovery"
use "wallaroo/core/routing"
use "wallaroo/core/source"
use "wallaroo/core/topology"
use "wallaroo_labs/bytes"
use cwm = "wallaroo_labs/connector_wire_messages"
use "wallaroo_labs/mort"
use "wallaroo_labs/time"

type _ProtoFsmState is (_ProtoFsmConnected | _ProtoFsmHandshake |
                        _ProtoFsmStreaming | _ProtoFsmError |
                        _ProtoFsmDisconnected)
primitive _ProtoFsmConnected
  fun apply(): U8 => 1
primitive _ProtoFsmHandshake
  fun apply(): U8 => 2
primitive _ProtoFsmStreaming
  fun apply(): U8 => 3
primitive _ProtoFsmError
  fun apply(): U8 => 4
primitive _ProtoFsmDisconnected
  fun apply(): U8 => 5


class _StreamState
  """
  A mutable SteamState class for local state management
  For sendable versions going outside of the notifier,
  use streamstate(): StreamState
  """
  var last_seen: PointOfReference  // last seen message id
  var last_acked: PointOfReference // last message id that was checkpointed
  var last_checkpoint_id: CheckpointId // last checkpoint id

  new ref create(last_seen': PointOfReference,
    last_acked': PointOfReference, last_checkpoint_id': CheckpointId)
=>
  last_seen = last_seen'
  last_acked = last_acked'
  last_checkpoint_id = last_checkpoint_id'

  fun val streamstate(): StreamState =>
    StreamState(last_seen, last_acked, last_checkpoint_id)

class val NotifyResult[In: Any val]
  """
  The type to use with Promise objects used to manage responses for the async
  parts of the stream_notify sequence
  """
  let source: ConnectorSource[In] tag
  let success: Bool
  let resume_from: PointOfReference

  new val create(source': ConnectorSource[In] tag,
    success': Bool, resume_from': PointOfReference)
  =>
    source = source'
    success = success'
    resume_from = resume_from'

class NotifyResultFulfill[In: Any val] is
  Fulfill[NotifyResult[In], None]

  let stream_id: StreamId
  let session_id: RoutingId

  new create(stream_id': StreamId, session_id': RoutingId)
  =>
    stream_id = stream_id'
    session_id = session_id'

  fun apply(t: NotifyResult[In]) =>
    t.source.stream_notify_result(session_id, t.success,
      stream_id, t.resume_from)

class val ConnectorSourceNotifyParameters[In: Any val]
  let pipeline_name: String
  let env: Env
  let auth: AmbientAuth
  let handler: FramedSourceHandler[In] val
  let runner_builder: RunnerBuilder
  let partitioner_builder: PartitionerBuilder
  let router: Router
  let metrics_reporter: MetricsReporter val
  let event_log: EventLog
  let target_router: Router
  let cookie: String
  let max_credits: U32
  let refill_credits: U32
  let host: String
  let service: String

  new val create(pipeline_name': String, env': Env,
    auth': AmbientAuth, handler': FramedSourceHandler[In] val,
    runner_builder': RunnerBuilder, partitioner_builder': PartitionerBuilder,
    router': Router, metrics_reporter': MetricsReporter val,
    event_log': EventLog, target_router': Router, cookie': String,
    max_credits': U32, refill_credits': U32, host': String, service': String)
  =>
    pipeline_name = pipeline_name'
    env = env'
    auth = auth'
    handler = handler'
    runner_builder = runner_builder'
    partitioner_builder = partitioner_builder'
    router = router'
    metrics_reporter = metrics_reporter'
    event_log = event_log'
    target_router = target_router'
    cookie = cookie'
    max_credits = max_credits'
    refill_credits = refill_credits'
    host = host'
    service = service'

class ConnectorSourceNotify[In: Any val]
  let source_id: RoutingId
  let _env: Env
  let _auth: AmbientAuth
  let _msg_id_gen: MsgIdGenerator = MsgIdGenerator
  var _header: Bool = true
  let _pipeline_name: String
  let _source_name: String
  let _handler: FramedSourceHandler[In] val
  let _runner: Runner
  var _router: Router
  let _metrics_reporter: MetricsReporter
  let _header_size: USize
  var _listener: ConnectorSourceListener[In]
  var _connector_source: ConnectorSource[In] ref

  // Barrier/checkpoint id tracking
  var _barrier_ongoing: Bool = false
  var _barrier_checkpoint_id: CheckpointId = 0

  // we need these for RestartMsg(host, port)
  var host: String
  var service: String

  // stream state management
  var _active_streams: Map[StreamId, _StreamState]= _active_streams.create()
  var _pending_notify: Set[StreamId] = _pending_notify.create()
  var _pending_close: Map[StreamId, _StreamState] = _pending_close.create()
  var _pending_relinquish: Map[StreamId, _StreamState] =
    _pending_relinquish.create()
  var _pending_acks: Array[(StreamId, PointOfReference)] =
    _pending_acks.create()

  var _session_active: Bool = false
  var _session_id: RoutingId = 0
  var _fsm_state: _ProtoFsmState = _ProtoFsmDisconnected
  let _cookie: String
  let _max_credits: U32
  let _refill_credits: U32
  var _credits: U32 = 0
  var _program_name: String = ""
  var _instance_name: String = ""
  var _prep_for_rollback: Bool = false
  let _debug_disconnect: Bool = false

  // Watermark !TODO! How do we handle this respecting per-connector-type
  // policies
  var _watermark_ts: U64 = 0

  new create(source_id': RoutingId,
    parameters: ConnectorSourceNotifyParameters[In],
    listener': ConnectorSourceListener[In], source': ConnectorSource[In] ref)
  =>
    source_id = source_id'
    _pipeline_name = parameters.pipeline_name
    _source_name = parameters.pipeline_name + " source"
    _env = parameters.env
    _auth = parameters.auth
    _handler = parameters.handler
    _runner = parameters.runner_builder(parameters.event_log, _auth, None,
      parameters.target_router, parameters.partitioner_builder)
    _router = parameters.router
    _metrics_reporter = recover iso parameters.metrics_reporter.clone() end
    _header_size = _handler.header_length()
    _cookie = parameters.cookie
    _max_credits = parameters.max_credits
    _refill_credits = parameters.refill_credits
    host = parameters.host
    service = parameters.service

    _listener = listener'
    _connector_source = source'

    ifdef "trace" then
      @printf[I32]("%s: max_credits = %lu, refill_credits = %lu\n".cstring(),
      __loc.type_name().cstring(), _max_credits, _refill_credits)
    end

  fun routes(): Map[RoutingId, Consumer] val =>
    _router.routes()

  fun ref received(source: ConnectorSource[In] ref, data: Array[U8] iso): Bool =>
    if _header then
      try
        let payload_size: USize = _handler.payload_length(consume data)?

        source.expect(payload_size)
        _header = false
      else
        Fail()
      end
      true
    else
      ifdef "trace" then
        @printf[I32](("Rcvd msg at " + _pipeline_name + " source\n").cstring())
      end
      _metrics_reporter.pipeline_ingest(_pipeline_name, _source_name)
      let ingest_ts = WallClock.nanoseconds()
      let pipeline_time_spent: U64 = 0
      let latest_metrics_id: U16 = 1

      received_connector_msg(source, consume data, latest_metrics_id,
        ingest_ts, pipeline_time_spent)
    end

  // TODO [source-migration]: Is this only ever used for rollback?
  // should it be renamed to be more specific?
  fun ref received_connector_msg(source: ConnectorSource[In] ref,
    data: Array[U8] iso,
    latest_metrics_id: U16,
    ingest_ts: U64,
    pipeline_time_spent: U64): Bool
  =>
    if _prep_for_rollback then
      // Anything that the connector sends us is ignored while we wait
      // for the rollback to finish.  Tell the connector to restart later.
      _send_restart()
      return _continue_perhaps(source)
    end

    _credits = _credits - 1
    if (_credits <= _refill_credits) and
        (_fsm_state is _ProtoFsmStreaming) then
      // Our client's credits are running low and we haven't replenished
      // them after barrier_complete() processing.  Replenish now.
      _send_ack()
    end

    try
      let data': Array[U8] val = consume data
      @printf[I32]("NH: decode data: %s\n".cstring(), _print_array[U8](data').cstring())
      let connector_msg = cwm.Frame.decode(consume data')?
      match connector_msg
      | let m: cwm.HelloMsg =>
        ifdef "trace" then
          @printf[I32]("TRACE: got HelloMsg\n".cstring())
        end
        if _fsm_state isnt _ProtoFsmConnected then
          @printf[I32]("ERROR: %s.received_connector_msg: state is %d\n".cstring(),
            __loc.type_name().cstring(), _fsm_state())
          Fail()
        end

        if m.version != "0.0.1" then
          @printf[I32]("ERROR: %s.received_connector_msg: unknown protocol version %s\n".cstring(),
            __loc.type_name().cstring(), m.version.cstring())
          return _to_error_state(source, "Unknown protocol version")
        end
        if m.cookie != _cookie then
          @printf[I32]("ERROR: %s.received_connector_msg: bad cookie %s\n".cstring(),
            __loc.type_name().cstring(), m.cookie.cstring())
          return _to_error_state(source, "Bad cookie")
        end

        // SLF TODO: add routing logic to handle
        // m.program_name and m.instance_name routing requirements
        // Right now, we assume that all messages are to be processed
        // by this connector's one & only pipeline as defined by the
        // app's pipeline definition.

        // process Hello: reply immediately with an Ok(credits, [], [])
        // reset _credits to _max_credits
        _credits = _max_credits
        _send_reply(_connector_source, cwm.OkMsg(_credits, [], []))
        _fsm_state = _ProtoFsmStreaming
        return _continue_perhaps(source)

      | let m: cwm.OkMsg =>
        ifdef "trace" then
          @printf[I32]("TRACE: got OkMsg\n".cstring())
        end
        return _to_error_state(source, "Invalid message: ok")

      | let m: cwm.ErrorMsg =>
        @printf[I32]("Client sent us ERROR msg: %s\n".cstring(),
          m.message.cstring())
        source.close()
        return _continue_perhaps(source)

      | let m: cwm.NotifyMsg =>
        ifdef "trace" then
          @printf[I32]("TRACE: got NotifyMsg: %lu %s %lu\n".cstring(),
            m.stream_id, m.stream_name.cstring(), m.point_of_ref)
        end
        if _fsm_state isnt _ProtoFsmStreaming then
          return _to_error_state(source, "Bad protocol FSM state")
        end

        if _pending_notify.contains(m.stream_id) or
           _active_streams.contains(m.stream_id) or
           _pending_close.contains(m.stream_id) or
           _pending_relinquish.contains(m.stream_id)
        then
          // This notifier is already handling this stream
          // So reject directly
          send_notify_ack(false, m.stream_id, m.point_of_ref)
        else
          _process_notify(where source=source, stream_id=m.stream_id,
            stream_name=m.stream_name, point_of_reference=m.point_of_ref)
        end

      | let m: cwm.NotifyAckMsg =>
        ifdef "trace" then
          @printf[I32]("TRACE: got NotifyAckMsg\n".cstring())
        end
        return _to_error_state(source, "Invalid message: notify_ack")

      | let m: cwm.MessageMsg =>
        ifdef "trace" then
          @printf[I32]("TRACE: got MessageMsg\n".cstring())
        end
        if _fsm_state isnt _ProtoFsmStreaming then
          return _to_error_state(source, "Bad protocol FSM state")
        end
        if not cwm.FlagsAllowed(m.flags) then
          return _to_error_state(source, "Bad MessageMsg flags")
        end

        let stream_id = m.stream_id
        var s = _StreamState( 0, 0, 0) // Will be overwritten below

        let decoded = if not _active_streams.contains(stream_id) then
          return _to_error_state(source, "Bad stream_id " + stream_id.string())
        else
          try
            s = _active_streams(stream_id)?
            ifdef "trace" then
              // TODO [source-migration]: Do we need to re-add pending_query to _StreamState?
              @printf[I32]("TRACE: STREAM pending %s base-p-o-r %llu last-msg-id %llu barrier-last-msg-id %llu barrier-ckpt-id %llu\n".cstring(), s.pending_query.string().cstring(), s.base_point_of_reference, s.last_message_id, s.barrier_last_message_id, s.barrier_checkpoint_id)
            end
            // TODO [source-migration]: Do we need to re-add pending_query to _StreamState?
            if s.pending_query then
              return _to_error_state(source, "Duplicate stream_id " + stream_id.string())
            end

            let msg_id' = if m.message_id is None then "<None>" else (m.message_id as cwm.MessageId).string() end
            let event_time' = if m.event_time is None then "<None>" else (m.event_time as cwm.EventTimeType).string() end
            let key' = if m.key is None then "<None>" else _print_array[U8](m.key as cwm.KeyBytes) end
            let message' = if m.message is None then "<None>" else _print_array[U8](m.message as cwm.MessageBytes) end
            ifdef "trace" then
              @printf[I32]("TRACE: MSG: stream-id %llu flags %u msg_id %s event_time %s key %s message %s\n".cstring(), stream_id, m.flags, msg_id'.cstring(), event_time'.cstring(), key'.cstring(), message'.cstring())
            end

            try
              @printf[I32]("NH: processing body 1\n".cstring())
              let msg_id = try
                m.message_id as cwm.MessageId
              else
                0
              end

              @printf[I32]("NH: processing body 2\n".cstring())
              if (msg_id > 0) and (msg_id <= s.last_message_id) then
                ifdef "trace" then
                  @printf[I32]("TRACE: MessageMsg: stale id in stream-id %llu flags %u msg_id %llu <= last_message_id %llu\n".cstring(), stream_id, m.flags, msg_id, s.last_message_id)
                end
                return _continue_perhaps(source)
              end

              @printf[I32]("NH: processing body 3\n".cstring())
              if cwm.Eos.is_set(m.flags) then
                @printf[I32]("NH: processing body 3: EOS\n".cstring())
                // Process EOS
                // 1. remove state from _active_streams
                try _active_streams.remove(stream_id)? end
                // 2. add state to _pending_close
                _pending_close.update(stream_id, s)
                // The rest happens asynchronously

                // TODO [source-migration]: implement this in checkpoint handler
                // 3. when checkpoint arrives, check if it acks up to
                //    _SteamState.last_message_id
                // 4. If not, go to 3 until next checkpoint, else continue
                // 5. Once _StreamState.last_message_id is checkpointed,
                //    start _relinquish behaviour


                // TODO [source-migration]: move this to relinquish fun
                _listener
                  .stream_update(stream_id, s.barrier_checkpoint_id,
                    s.barrier_last_message_id, msg_id, None)
              end

              @printf[I32]("NH: processing body 4\n".cstring())
              if cwm.Boundary.is_set(m.flags) then
                None
              else
                try
                  let bytes = match (m.message as cwm.MessageBytes)
                  | let str: String      => str.array()
                  | let b: Array[U8] val => b
                  end
                  if cwm.Eos.is_set(m.flags) or (bytes.size() == 0) then
                    None
                  else
                    _handler.decode(bytes)?
                  end
                else
                  if m.message is None then
                    return _to_error_state(source, "No message bytes and BOUNDARY not set")
                  end
                  @printf[I32](("Unable to decode message at " + _pipeline_name + " source\n").cstring())
                  ifdef debug then
                    Fail()
                  end
                  return _to_error_state(source, "Unable to decode message")
                end
              end

              // TODO:
              // 6. What did I forget?  See received_old_school() for hints:
              //    it isn't perfect but it "works" at demo quality.
            else
              @printf[I32]("NH: processing body failed!\n".cstring())
              Fail()
            end
          else
            return _to_error_state(source, "Unknown StreamId")
          end
        end

        ifdef "trace" then
          @printf[I32](("Msg decoded at " + _pipeline_name +
            " source\n").cstring())
        end
        // TODO [source-migration]: Do we need to re-add pending_query to _StreamState?
        if s.pending_query then // assert sanity
          Fail()
        end
        let key_string =
          match m.key
          | None =>
            ""
          | let str: String val =>
            str
          | let a: Array[U8] val =>
            let k' = recover trn String(a.size()) end
            for c in a.values() do
              k'.push(c)
            end
            consume k'
          end
        _run_and_subsequent_activity(latest_metrics_id, ingest_ts,
          pipeline_time_spent, key_string, source, decoded, s,
          m.message_id, m.flags)

      | let m: cwm.AckMsg =>
        ifdef "trace" then
          @printf[I32]("TRACE: got AckMsg\n".cstring())
        end
        return _to_error_state(source, "Invalid message: ack")

      | let m: cwm.RestartMsg =>
        ifdef "trace" then
          @printf[I32]("TRACE: got RestartMsg\n".cstring())
        end
        return _to_error_state(source, "Invalid message: restart")

      end
    else
      @printf[I32](("Unable to decode message at " + _pipeline_name +
        " source\n").cstring())
      ifdef debug then
        Fail()
      end
      return _to_error_state(source, "Unable to decode message")
    end
    _continue_perhaps(source)

  fun ref _run_and_subsequent_activity(latest_metrics_id: U16,
    ingest_ts: U64,
    pipeline_time_spent: U64,
    key_string: String val,
    source: ConnectorSource[In] ref,
    decoded: (In val| None val),
    s: _StreamState,
    message_id: (cwm.MessageId|None),
    flags: cwm.Flags): Bool
   =>
    let decode_end_ts = WallClock.nanoseconds()
    _metrics_reporter.step_metric(_pipeline_name,
      "Decode Time in Connector Source", latest_metrics_id, ingest_ts,
      decode_end_ts)
    let latest_metrics_id' = latest_metrics_id + 1

    let msg_uid = _msg_id_gen()

    // TODO: We need a way to determine the key based on the policy
    // for any particular connector. For example, the Kafka connector
    // needs a way to provide the Kafka key here.

    let initial_key =
      if key_string isnt None then
        key_string
      else
        msg_uid.string()
      end

    // TOOD: We need a way to assign watermarks based on the policy
    // for any particular connector.
    if ingest_ts > _watermark_ts then
      _watermark_ts = ingest_ts
    end

    (let is_finished, let last_ts) =
      match decoded
      | None =>
        (true, ingest_ts)
      | let d: In =>
        _runner.run[In](_pipeline_name, pipeline_time_spent, d,
          consume initial_key, ingest_ts, _watermark_ts, source_id,
          source, _router, msg_uid, None, decode_end_ts,
          latest_metrics_id, ingest_ts, _metrics_reporter)
      end

    match message_id
    | let m_id: PointOfReference =>
      if not (cwm.Ephemeral.is_set(flags) or
        cwm.UnstableReference.is_set(flags)) then
        s.last_message_id = m_id
      end
    end

    if is_finished then
      let end_ts = WallClock.nanoseconds()
      let time_spent = end_ts - ingest_ts

      ifdef "detailed-metrics" then
        _metrics_reporter.step_metric(_pipeline_name,
          "Before end at Connector Source", 9999,
          last_ts, end_ts)
      end

      _metrics_reporter.pipeline_metric(_pipeline_name, time_spent +
        pipeline_time_spent)
      _metrics_reporter.worker_metric(_pipeline_name, time_spent)
    end

    _continue_perhaps(source)

  fun ref _continue_perhaps(source: ConnectorSource[In] ref): Bool =>
    @printf[I32]("NH: _continue_perhaps: %s\n".cstring(),
      _header_size.string().cstring())
    source.expect(_header_size)
    _header = true
    _continue_perhaps2()

  fun ref _continue_perhaps2(): Bool =>
    ifdef linux then
      true
    else
      false
    end

  fun ref update_router(router': Router) =>
    _router = router'

  fun ref update_boundaries(obs: box->Map[String, OutgoingBoundary]) =>
    match _router
    | let p_router: StatePartitionRouter =>
      _router = p_router.update_boundaries(_auth, obs)
    else
      ifdef "trace" then
        @printf[I32](("FramedSourceNotify doesn't have StatePartitionRouter." +
          " Updating boundaries is a noop for this kind of Source.\n")
          .cstring())
      end
    end

  fun ref accepted(source: ConnectorSource[In] ref, session_id: RoutingId) =>
    @printf[I32]((_source_name + ": accepted a connection\n").cstring())
    if _fsm_state isnt _ProtoFsmDisconnected then
      @printf[I32]("ERROR: %s.connected: state is %d\n".cstring(),
        __loc.type_name().cstring(), _fsm_state())
      Fail()
    end
    // reset/clear local state
    _fsm_state = _ProtoFsmConnected
    _header = true
    _session_active = true
    _session_id = session_id
    // TODO [source-migration]: be careful about pushing state back to registry
    _pending_notify.clear()  // have to be resent by external connector
    _active_streams.clear()  // relinquish? should this be part of closed?
    _pending_close.clear()   // should be part of closed
    _pending_relinquish.clear()  // should be resent?

    _credits = _max_credits
    _prep_for_rollback = false
    source.expect(_header_size)

  fun ref closed(source: ConnectorSource[In] ref) =>
    @printf[I32]("ConnectorSource connection closed 0x%lx\n".cstring(), source)
    _session_active = false
    _fsm_state = _ProtoFsmDisconnected
    _clear_streams()

  fun ref throttled(source: ConnectorSource[In] ref) =>
    @printf[I32]("%s.throttled: %s Experiencing backpressure!\n".cstring(),
      __loc.type_name().cstring(), _pipeline_name.cstring())
    Backpressure.apply(_auth) // TODO: appropriate?

  fun ref unthrottled(source: ConnectorSource[In] ref) =>
    @printf[I32]("%s.unthrottled: %s Releasing backpressure!\n".cstring(),
      __loc.type_name().cstring(), _pipeline_name.cstring())
    Backpressure.release(_auth) // TODO: appropriate?

  fun ref connecting(conn: ConnectorSource[In] ref, count: U32) =>
    """
    Called if name resolution succeeded for a ConnectorSource and we are now
    waiting for a connection to the server to succeed. The count is the number
    of connections we're trying. The notifier will be informed each time the
    count changes, until a connection is made or connect_failed() is called.
    """
    Fail()

  fun ref connected(conn: ConnectorSource[In] ref) =>
    """
    Called when we have successfully connected to the server.
    """
    Fail()

  fun ref connect_failed(conn: ConnectorSource[In] ref) =>
    """
    Called when we have failed to connect to all possible addresses for the
    server. At this point, the connection will never be established.
    """
    Fail()

  fun ref expect(conn: ConnectorSource[In] ref, qty: USize): USize =>
    """
    Called when the connection has been told to expect a certain quantity of
    bytes. This allows nested notifiers to change the expected quantity, which
    allows a lower level protocol to handle any framing (e.g. SSL).
    """
    qty

  fun ref _clear_streams() =>
    // TODO [source-migration]: relinquish all streams, then clear them out
    // _pending_notify
    // _active_streams
    // _pending_close
    // _relinquish (it's okay to double send here)
    for (stream_id, s) in _active_streams.pairs() do
      try
        _listener
        .stream_update(stream_id, s.barrier_checkpoint_id,
          s.barrier_last_message_id, s.last_message_id, None)
      else
        Fail()
      end
    end
    _active_streams.clear()

  fun create_checkpoint_state(): Array[ByteSeq val] val =>
    // recover val ["<{stand-in for state for ConnectorSource with routing id="; source_id.string(); "}>"] end

    // TODO [source-migration]: include the pending_close, pending_relinquish
    // states as well
    let w: Writer = w.create()
    for (stream_id, s) in _active_streams.pairs() do
      w.u64_be(stream_id)
      w.u64_be(s.barrier_checkpoint_id)
      w.u64_be(s.barrier_last_message_id)
      w.u64_be(s.last_message_id)
    end
    w.done()

  fun ref prepare_for_rollback() =>
    if _session_active then
      _clear_streams()
      _send_restart()
      _prep_for_rollback = true
      ifdef "trace" then
        @printf[I32]("TRACE: %s.%s\n".cstring(), __loc.type_name().cstring(), __loc.method_name().cstring())
      end
    end

  fun ref rollback(checkpoint_id: CheckpointId, payload: ByteSeq val) =>
    ifdef "trace" then
      @printf[I32]("TRACE: %s.%s(%lu)\n".cstring(), __loc.type_name().cstring(), __loc.method_name().cstring(), checkpoint_id)
    end

    // TODO [source-migration]: load pending_close, pending_relinquish states as
    // well
    let r = Reader
    r.append(payload)
    try
      while true do
        let stream_id = r.u64_be()?
        let barrier_checkpoint_id = r.u64_be()?
        let barrier_last_message_id = r.u64_be()?
        let last_message_id = r.u64_be()?
        ifdef "trace" then
          @printf[I32]("TRACE: read = s-id %lu b-ckp-id %lu b-l-msg-id %lu l-msg-id %lu\n".cstring(),
          stream_id, barrier_checkpoint_id, barrier_last_message_id, last_message_id)
        end
        _listener
        .stream_update(stream_id, barrier_checkpoint_id,
            barrier_last_message_id, last_message_id, None)
      end
    end
    _prep_for_rollback = false
    _send_restart()

  fun ref initiate_barrier(checkpoint_id: CheckpointId) =>
    _barrier_ongoing = true
    Invariant(checkpoint_id > _barrier_checkpoint_id)
    _barrier_checkpoint_id = checkpoint_id

    if _session_active then
      for s in _active_streams.values() do
        s.last_checkpoint_id = checkpoint_id
        s.last_acked = s.last_seen

        ifdef "trace" then
          @printf[I32]("TRACE: %s.%s(%lu) _barrier_last_message_id = %lu\n".cstring(),
            __loc.type_name().cstring(), __loc.method_name().cstring(),
            checkpoint_id, s.barrier_last_message_id)
        end
      end
      for s in _pending_close.values() do
        s.last_checkpoint_id = checkpoint_id
        s.last_acked = s.last_seen

        ifdef "trace" then
          @printf[I32]("TRACE: %s.%s(%lu) _barrier_last_message_id = %lu\n".cstring(),
            __loc.type_name().cstring(), __loc.method_name().cstring(),
            checkpoint_id, s.barrier_last_message_id)
        end
      end
    end

  fun ref barrier_complete(checkpoint_id: CheckpointId) =>
    // update barrier state and check checkpoint_id matches our local knowledge
    _barrier_ongoing = false
    Invariant(checkpoint_id == _barrier_checkpoint_id)

    if _session_active then
      for (stream_id, s) in _active_streams.pairs() do
        try
          _listener
            .stream_update(
              stream_id, s.last_checkpoint_id, s.last_acked,
              s.last_seen,
              _connector_source)
          _pending_acks.push((stream_id, s.last_acked))
        else
          Fail()
        end
      end

      // TODO [source-migration]: _StreamState needs to be sendable if we're going to make this array sendable
      let to_relinquish = recover iso Array[(StreamId, _StreamState)] end

      // process acks for EOS/_pending_close streams
      for (stream_id, s) in _pending_close.pairs() do
        try
          _listener
            .stream_update(
              stream_id, s.last_checkpoint_id, s.last_acked,
              s.last_seen,
              _connector_source)
          _pending_acks.push((stream_id, s.last_acked))
          to_relinquish.push((stream_id, s))
        else
          Fail()
        end
      end
      // clear _pending_close
      _pending_close.clear()
      // process new stream relinquish requests
      _relinquish_streams(consume to_relinquish)

      if _fsm_state is _ProtoFsmStreaming then
        // Send an Ack message to replenish credits
        _send_ack()
      end

      // SLF TODO: this if clause is for debugging purposes only
      if _debug_disconnect and ((checkpoint_id % 5) == 4) then
        // SLF TODO: the Python side of the world raises an exception when
        // it gets an ErrorMsg, and the rest of the code is fragile when
        // that exception is not caught.
        // _to_error_state(_connector_source, "BYEBYE")

        _send_restart()
      end
    end

  fun ref _relinquish_streams(streams: Array[(StreamId, _StreamState)] val) =>
    // add the stream to _pending_reliqnuish
    for (i, (sid, state)) in streams.pairs() do
      // TODO [source-migration]: _StreamState is a val here and we need a ref for _pending_relinquish
      _pending_relinquish(sid) = state
      _listener.stream_relinquish(sid, state.last_acked)
    end

  // TODO [source-migration]: implement this
  fun ref stream_relinquish_result(session_id: RoutingId, success: Bool,
    stream_id: StreamId, point_of_reference: PointOfReference)
  =>
    None

  fun ref _process_notify(source: ConnectorSource[In] ref, stream_id: StreamId,
    stream_name: String, point_of_reference: PointOfReference)
  =>
    // add to _pending_notify
    _pending_notify.set(stream_id)

    // create a promise for handling result
    let promise = Promise[NotifyResult[In]]
    promise.next[None](recover NotifyResultFulfill[In](stream_id, _session_id) end)

    // send request to take ownership of this stream id
    let request_id = ConnectorStreamNotifyId(stream_id, _session_id)
    try
      // TODO [source-migration]: do we need the stream name here? currently required in the listener's stream_notify function
      _listener.stream_notify(request_id, stream_id, promise)
    end

  fun ref stream_notify_result(session_id: RoutingId, success: Bool,
    stream_id: StreamId, point_of_reference: PointOfReference)
  =>
    if (session_id != _session_id) or (not _session_active) then
      // This is a reply from a query that we'd sent in a prior TCP
      // connection, or else the TCP connection is closed now,
      // so ignore it.
      // If the connection has been closed, any state about this query would
      // have already been purged from any local state
      return
    end

    ifdef "trace" then
      @printf[I32]("TRACE: %s.%s(%s, %lu, p-o-r %lu)\n".cstring(),
        __loc.type_name().cstring(), __loc.method_name().cstring(),
        success.string().cstring(),
        stream_id, point_of_reference)
    end

    // remove entry from _pending_notify set
    _pending_notify.unset(stream_id)
    if success then
      // create _StreamState to place in _active_streams
      let s = _StreamState(point_of_reference, point_of_reference,
        _barrier_checkpoint_id)
      _active_streams(stream_id) = s
    end
    // send response either way
    send_notify_ack(success, stream_id, point_of_reference)

  fun ref _to_error_state(source: (ConnectorSource[In] ref|None), msg: String): Bool
  =>
    _send_reply(source, cwm.ErrorMsg(msg))

    _fsm_state = _ProtoFsmError
    try (source as ConnectorSource[In] ref).close() else Fail() end
    _continue_perhaps2()

  fun ref send_notify_ack(success: Bool, stream_id: StreamId,
    point_of_reference: PointOfReference)
  =>
    let m = cwm.NotifyAckMsg(success, stream_id, point_of_reference)
    _send_reply(_connector_source, m)

  fun ref _send_ack() =>
    let new_credits = _max_credits - _credits
    // destructively read the pending acks
    // and send the ack to the connector
    let acks = recover iso Array[(U64, U64)] end
    for (i, v) in _pending_acks.pairs() do
      acks.push(v)
      _pending_acks.remove(i, 1)
    end
    _send_reply(_connector_source, cwm.AckMsg(new_credits, consume acks))
    _credits = _credits + new_credits

  fun ref _send_restart() =>
    ifdef "trace" then
      @printf[I32]("TRACE: %s.%s()\n".cstring(),
        __loc.type_name().cstring(), __loc.method_name().cstring())
    end
    _send_reply(_connector_source, cwm.RestartMsg(host + ":" + service))
    _connector_source.close()
    // The .close() method ^^^ calls our closed() method which will
    // twiddle all of the appropriate state variables.

  // TODO [source-migration]: why pass source here instead of using
  // var _connector_source?
  fun _send_reply(source: (ConnectorSource[In] ref|None), msg: cwm.Message) =>
    match source
    | let s: ConnectorSource[In] ref =>
      // write the frame data and length encode it
      let w1: Writer = w1.create()
      let b1 = cwm.Frame.encode(msg, w1)
      s.writev_final(Bytes.length_encode(b1))
    else
      Fail()
    end

  fun _print_array[A: Stringable #read](array: ReadSeq[A]): String =>
    """
    Generate a printable string of the contents of the given readseq to use in
    error messages.
    """
    "[len=" + array.size().string() + ": " + ", ".join(array.values()) + "]"
