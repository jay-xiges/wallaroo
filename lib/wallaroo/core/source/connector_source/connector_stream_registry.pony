/*

Copyright (C) 2016-2017, Wallaroo Labs
Copyright (C) 2016-2017, The Pony Developers
Copyright (c) 2014-2015, Causality Ltd.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/


use "collections"
use "promises"
use "wallaroo/core/checkpoint"
use "wallaroo/core/common"
use "wallaroo/core/invariant"
use "wallaroo/core/messages"
use "wallaroo/core/network"
use "wallaroo_labs/mort"
use "wallaroo_labs/time"

// Connector Types
type StreamId is U64
type PointOfReference is U64

class val StreamTuple
  let id: StreamId
  let name: String
  let last_acked: PointOfReference

  new val create(stream_id: StreamId, stream_name: String,
    point_of_ref: PointOfReference)
  =>
    name = stream_name
    id = stream_id
    last_acked = point_of_ref

// TODO [source-migration]: Add _request_leader() function to get existing
// leader during a join
// TODO [source-migration]: Either get a list of "existing" workers to query for
// request_leader, or ask ALL workers and currently-joining workers do not
// respond. or something else?
// TODO [source-migration]: add var _is_joining: Bool to determine above logic
class GlobalConnectorStreamRegistry[In: Any val]
  let _listener: ConnectorSourceListener[In] tag
  var _worker_name: String
  let _source_name: String
  let _connections: Connections
  let _source_addr: (String, String)
  var _is_leader: Bool = false
  var _is_relinquishing: Bool = false
  var _is_joining: Bool
  var _leader_name: String = "Initializer"
  var _active_streams: Map[StreamId, WorkerName] = _active_streams.create()
  var _inactive_streams: Map[StreamId, StreamTuple] =  _inactive_streams.create()
  var _source_addrs: Map[WorkerName, (String, String)] =
    _source_addrs.create()
  let _workers_set: Set[WorkerName] = _workers_set.create()
  let _pending_notify_promises:
    Map[ConnectorStreamNotifyId, (Promise[NotifyResult[In]], ConnectorSource[In])] =
      _pending_notify_promises.create()
  var _local_registry: (LocalConnectorStreamRegistry[In] | None ) = None

  new create(listener: ConnectorSourceListener[In],
    worker_name: WorkerName, source_name: String,
    connections: Connections, host: String, service: String,
    workers_list: Array[WorkerName] val, is_joining: Bool)
  =>
    _listener = listener
    _worker_name = worker_name
    _source_name = source_name
    _connections = connections
    _source_addr = (host, service)
    _is_joining = is_joining
    for worker in workers_list.values() do
      _workers_set.set(worker)
    end
    _elect_leader()

  fun ref set_local_registry(local_registry: LocalConnectorStreamRegistry[In] ref) =>
    _local_registry = local_registry

  //////////////////////////
  // MANAGE PENDING REQUESTS
  //////////////////////////
  fun ref purge_pending_requests(session_id: RoutingId) =>
    """
    Delete promises for older session ids, since they will be
    noops at the source.
    """
    for req in _pending_notify_promises.keys() do
      if req.session_id == session_id then
        try _pending_notify_promises.remove(req)? end
      end
    end

  fun ref process_new_leader_msg(msg: ConnectorNewLeaderMsg) =>
    if not _is_leader then
      _leader_name = msg.leader_name
    else
      @printf[I32](("GlobalConnectorStreamRegistry leader received a new leader"
        + " message from non-leader: %s.\n").cstring(),
        msg.leader_name.cstring())
      Fail()
    end

  fun ref process_leader_state_received_msg(
    msg: ConnectorLeaderStateReceivedAckMsg)
  =>
    if _is_leader and _is_relinquishing then
      _leader_name = msg.leader_name
      _is_leader = false
      _active_streams.clear()
      _inactive_streams.clear()
      _source_addrs.clear()
      _pending_notify_promises.clear()
      _is_relinquishing = false
      @printf[I32](("Connector Leadership relinquish complete. New Leader: " +
      msg.leader_name + "\n").cstring())
    else
      @printf[I32](("Not a Connector leader but received a " +
        "ConnectorLeaderStateReceivedAckMsg from " + msg.leader_name +
        "\n").cstring())
    end

  fun ref relinquish_leadership(new_leader_name: WorkerName) =>
    if _is_leader then
      _initiate_leadership_relinquishment(new_leader_name)
    else
      @printf[I32](("GlobalConnectorStreamRegistry received a relinquish " +
        "leadership message, from %s but is not the current leader.\n")
        .cstring(), new_leader_name.cstring())
    end

  fun ref process_leadership_relinquish_msg(
    msg: ConnectorLeadershipRelinquishMsg)
  =>
    _accept_leadership_state(msg.worker_name, msg.active_streams,
      msg.inactive_streams, msg.source_addrs)

  fun ref _accept_leadership_state(worker_name: WorkerName,
    active_streams: Map[StreamId, WorkerName] val,
    inactive_streams: Map[StreamId, StreamTuple] val,
    source_addrs: Map[WorkerName, (String, String)] val)
  =>
    // update active stream map
    let active_streams_copy = Map[StreamId, WorkerName]()
    for (k,v) in active_streams.pairs() do
      active_streams_copy(k) = v
    end
    _active_streams = active_streams_copy

    // update inactive stream map
    let inactive_streams_copy = Map[StreamId, StreamTuple]()
    for (k,v) in inactive_streams.pairs() do
      inactive_streams_copy(k) = v
    end
    _inactive_streams = inactive_streams_copy

    // update source_addrs
    let source_addrs_copy = Map[WorkerName, (String, String)]()
    for (k,v) in source_addrs.pairs() do
      source_addrs_copy(k) = v
    end
    _source_addrs = source_addrs_copy

    // update leader state
    _is_leader = true
    _leader_name = _worker_name
    // send ack
    _send_leader_state_received_ack(worker_name)
    _broadcast_new_leader()

  fun ref process_add_source_addr_msg(msg: ConnectorAddSourceAddrMsg) =>
    if _is_leader then
      _source_addrs(msg.worker_name) = (msg.host, msg.service)
    else
      @printf[I32](("GlobalConnectorStreamRegistry received a add_source_addr "
        + "message from %s but is not the current leader.\n").cstring(),
        msg.worker_name.cstring())
    end

  fun ref process_leader_name_request_msg(msg: ConnectorLeaderNameRequestMsg)
  =>
    // !TODO! [post-source-migration]: We should only process this message on
    // the Initializer. This should be updated once the Initializer
    // loses "special" status.
    if _worker_name == "Initializer" then
      _connections.connector_leader_name_response(msg.worker_name,
        _leader_name, msg.source_name())
    else
      Fail()
    end

  fun ref process_leader_name_response_msg(msg: ConnectorLeaderNameResponseMsg)
  =>
    _leader_name = msg.leader_name
    _is_joining = false
    // TODO [source-migration]: should the global registry be able to notify
    // the SourceListener (via local registry or some other fashion) that
    // they can update their _is_joining state and subsequently
    // "report_initialized" or should this be done via a Promise?
    try
      (_local_registry as LocalConnectorStreamRegistry[In]).set_joining(false)
    else
      Fail()
    end
    _listener.report_initialized()

  fun ref add_worker(worker_name: WorkerName) =>
    // TODO [post-source-migration-3]: we are lazily not re-electing leader on grow,
    // should we?
    _workers_set.set(worker_name)

  fun ref remove_worker(worker_name: WorkerName) =>
    // Relinquish streams should have happened before this is called
    // by a departing worker
      _workers_set.unset(worker_name)
    if _is_leader and (worker_name == _worker_name) then
      try
        let new_leader_name = _leader_from_workers_list()?
        relinquish_leadership(new_leader_name)
      else
        @printf[I32](("GlobalConnectorStreamRegistry could not determine "
        + "the new leader. Exiting.\n").cstring())
      end
    end

  ////////////////////////////
  // INTERNAL STATE MANAGEMENT
  ////////////////////////////
  fun ref _inactivate_stream(stream: StreamTuple) ? =>
    ifdef debug then
      @printf[I32](("%s ::: GlobalConnectorStreamRegistry._inactivate_stream("
        + "StreamTuple(%s, %s, %s))\n").cstring(),
        WallClock.seconds().string().cstring(),
        stream.id.string().cstring(), stream.name.cstring(),
        stream.last_acked.string().cstring())
    end
    // fail if not already active
    _active_streams.remove(stream.id)?
    _inactive_streams(stream.id) = stream

  fun ref _activate_stream(stream: StreamTuple, worker_name: WorkerName):
    StreamTuple ?
  =>
    // fail if not already in inactive
    let s = _inactive_streams.remove(stream.id)?
    _active_streams(stream.id) = worker_name
    s._2

  fun ref _new_stream(stream: StreamTuple, worker_name: WorkerName) ? =>
    // fail if already in inactive or active
    if _inactive_streams.contains(stream.id) then error end
    if _active_streams.contains(stream.id) then error end
    _active_streams(stream.id) = worker_name

  ////////////////
  // STREAM_NOTIFY
  ////////////////
  fun ref stream_notify(request_id: ConnectorStreamNotifyId,
    stream: StreamTuple, promise: Promise[NotifyResult[In]],
    connector_source: ConnectorSource[In] tag)
  =>
    """
    local->global stream_notify
    """
    if _is_leader then
      (let success, let stream') = try
          // new
          _new_stream(stream, _worker_name)?
          (true, stream)
        else
          try
            // already inactive or active: activate
            (true, _activate_stream(stream, _worker_name)?)
          else
            // error: already active
            (false, stream)
          end
        end
      // respond to local registry
      try (_local_registry as LocalConnectorStreamRegistry[In])
            .stream_notify_global_result(success,
              stream', promise, connector_source)
      else
        Fail()
      end
    else
      // Not leader, go over conections to leader worker
      _pending_notify_promises(request_id) = (promise, connector_source)
      _connections.connector_stream_notify(_leader_name, _worker_name,
        _source_name, stream, request_id)
    end

  fun ref process_stream_notify_msg(msg: ConnectorStreamNotifyMsg)
  =>
    """
    worker->leader stream_notify
    """
    if _is_leader then
      (let success, let stream') = try
          // new
          _new_stream(msg.stream, msg.worker_name)?
          (true, msg.stream)
        else
          // already active or inactive
          try
            // activate
            (true, _activate_stream(msg.stream, msg.worker_name)?)
          else
            // error: already active
            (false, msg.stream)
          end
        end
      _connections.connector_respond_to_stream_notify(msg.worker_name, msg.source_name(),
        success, stream', msg.request_id)
    else
      @printf[I32](("Non-leader worker received a stream notify request."
        + " This indicates an invalid leader state at " +
        msg.worker_name + "\n").cstring())
      Fail()
    end

  fun ref process_stream_notify_response_msg(
    msg: ConnectorStreamNotifyResponseMsg)
  =>
    """
    leader->worker stream_notify_result
    """
    try
      (let id, (let promise, let source)) =
        _pending_notify_promises.remove(msg.request_id)?
      try (_local_registry as LocalConnectorStreamRegistry[In])
        .stream_notify_global_result(msg.success, msg.stream,
          promise, source)
      else
        Fail()
      end
    else
      ifdef debug then
        @printf[I32](("GlobalConnectorStreamRegistry received a stream_ "
          + "notify reponse message for an unknown request."
          +" Ignoring.\n").cstring())
      end
    end

  ////////////////////
  // STREAM RELINQUISH
  ////////////////////
  fun ref streams_relinquish(streams: Array[StreamTuple] val) =>
    """
    Process a stream relinquish request from this worker

    local->global stream_relinquish
    """
    if _is_leader then
      for stream in streams.values() do
        Invariant(_active_streams.get_or_else(stream.id, _worker_name) ==
          _worker_name)
        try
          // assume active, try inactivate
          _inactivate_stream(stream)?
        end
      end
    else
      _connections.connector_streams_relinquish(_leader_name, _worker_name,
        _source_name, streams)
    end

  fun ref process_streams_relinquish_msg(msg: ConnectorStreamsRelinquishMsg) =>
    """
    Process a stream relinquish message from another worker

    worker->leader.stream_relinquish
    """
    if _is_leader then
      streams_relinquish(msg.streams)
    else
      @printf[I32](("Non-leader worker received a streams relinquish request."
        + " This indicates an invalid leader state at " +
        msg.worker_name + "\n").cstring())
      Fail()
    end

  //////////////////////
  //
  //////////////////////
  fun ref contains_notify_request(request_id: ConnectorStreamNotifyId): Bool =>
    _pending_notify_promises.contains(request_id)

  fun ref complete_leader_state_relinquish(new_leader_name: WorkerName) =>
    _relinquish_leader_state(new_leader_name)

  fun ref _relinquish_leader_state(new_leader_name: WorkerName) =>
    _active_streams = Map[StreamId, WorkerName]()
    _inactive_streams = Map[StreamId, StreamTuple]()
    _source_addrs = Map[WorkerName, (String, String)]()
    _is_leader = false
    _leader_name = new_leader_name

  fun ref _elect_leader() =>
    if _is_joining then
      // !TODO! [source-migration]: we're currently deferring leader request to
      // the Initializer due to the fact that it still holds "special" status.
      // This should be updated to request from a non-joining worker via a
      // different protocol in the near future.
      _connections.connector_leader_name_request(_worker_name, _source_name,
        "Initializer")
    else
      try
        let leader_name = _leader_from_workers_list()?
        if (leader_name == _worker_name) then
          _initiate_leader_state()
        else
          _leader_name = leader_name
          _send_leader_source_address()
        end
      else
        // unable to elect a leader
        Fail()
      end
    end

  fun ref _leader_from_workers_list(): WorkerName ? =>
    let workers_list = Array[WorkerName]
    for worker in _workers_set.values() do
      workers_list.push(worker)
    end
    let sorted_worker_names =
      Sort[Array[WorkerName], WorkerName](workers_list)
    sorted_worker_names(0)?

  fun ref _initiate_leader_state() =>
    _is_leader = true
    _leader_name = _worker_name
    _active_streams = Map[StreamId, WorkerName]()
    _inactive_streams = Map[StreamId, StreamTuple]()
    _source_addrs = Map[WorkerName, (String, String)]()
    _source_addrs(_worker_name) = (_source_addr._1, _source_addr._2)

  fun ref _send_leader_source_address() =>
    try
      let leader_name = _leader_from_workers_list()?
      _connections.connector_add_source_addr(leader_name, _worker_name,
        _source_name, _source_addr._1, _source_addr._2)
    else
      // Could not retrieve a leader
      Fail()
    end

  fun ref _send_leader_state_received_ack(worker_name: WorkerName) =>
    _connections.connector_leader_state_received_ack(_leader_name, worker_name, _source_name)

  fun ref _broadcast_new_leader() =>
    let workers_list_size = _workers_set.size()
    let workers_list = recover trn Array[WorkerName](workers_list_size) end
    for worker in _workers_set.values() do
      workers_list.push(worker)
    end

    _connections.connector_broadcast_new_leader(
      _worker_name, _source_name, consume workers_list)

  fun ref _initiate_leadership_relinquishment(new_leader_name: WorkerName) =>
    _is_relinquishing = true
    let active_streams_copy = recover trn Map[StreamId, WorkerName] end
    for (k,v) in _active_streams.pairs() do
      active_streams_copy(k) = v
    end
    let inactive_streams_copy = recover trn Map[StreamId, StreamTuple] end
    for (k,v) in _inactive_streams.pairs() do
      inactive_streams_copy(k) = v
    end
    let source_addrs_copy = recover trn
      Map[WorkerName, (String, String)]
    end
    for (k,v) in _source_addrs.pairs() do
      source_addrs_copy(k) = v
    end

    _connections.connector_leadership_relinquish_state(
      new_leader_name, _worker_name, _source_name,
      consume active_streams_copy, consume inactive_streams_copy,
      consume source_addrs_copy)


class ActiveStreamTuple[In: Any val]
  let id: StreamId
  let name: String
  var source: (ConnectorSource[In] | None)
  var last_acked: PointOfReference
  var last_seen: PointOfReference

  new create(stream_id': StreamId, stream_name': String,
    last_acked': PointOfReference, last_seen': PointOfReference,
    source': (ConnectorSource[In] | None))
  =>
    id = stream_id'
    name = stream_name'
    source = source'
    last_acked = last_acked'
    last_seen = last_seen'

class LocalConnectorStreamRegistry[In: Any val]
   // Global Stream Registry
  let _global_registry: GlobalConnectorStreamRegistry[In] ref

  // Local stream registry
  // (stream_name, connector_source, last_acked_por, last_seen_por)
  let _active_streams: Map[StreamId, ActiveStreamTuple[In]] =
    _active_streams.create()
  let _source_name: String
  var _is_joining: Bool

  // ConnectorSourceListener
  let _listener: ConnectorSourceListener[In] tag

  new ref create(listener: ConnectorSourceListener[In] tag,
    worker_name: WorkerName, source_name: String,
    connections: Connections, host: String, service: String,
    workers_list: Array[WorkerName] val, is_joining: Bool)
  =>
    _listener = listener
    _source_name = source_name
    _is_joining = is_joining
    _global_registry = _global_registry.create(_listener, worker_name,
      source_name, connections, host, service, workers_list, _is_joining)
    _global_registry.set_local_registry(this)

  ///////////////////
  // MESSAGE HANDLING
  ///////////////////

  fun ref listener_msg_received(msg: SourceListenerMsg) =>
    // we only care for messages that belong to this source name
    if (msg.source_name() == _source_name) then
      match msg
      |  let m: ConnectorStreamNotifyMsg =>
        _global_registry.process_stream_notify_msg(m)

      | let m: ConnectorStreamNotifyResponseMsg =>
        _global_registry.process_stream_notify_response_msg(m)

      | let m: ConnectorStreamsRelinquishMsg =>
        _global_registry.process_streams_relinquish_msg(m)

      | let m: ConnectorLeadershipRelinquishMsg =>
        _global_registry.process_leadership_relinquish_msg(m)

      | let m: ConnectorAddSourceAddrMsg =>
        _global_registry.process_add_source_addr_msg(m)

      | let m: ConnectorNewLeaderMsg =>
        _global_registry.process_new_leader_msg(m)

      | let m: ConnectorLeaderStateReceivedAckMsg =>
        _global_registry.process_leader_state_received_msg(m)

      | let m: ConnectorLeaderNameRequestMsg =>
        _global_registry.process_leader_name_request_msg(m)

      | let m: ConnectorLeaderNameResponseMsg =>
        _global_registry.process_leader_name_response_msg(m)

      end
    else
      @printf[I32](("**Dropping message** _pipeline_name: " +
         _source_name +  " =/= source_name: " + msg.source_name() + " \n")
        .cstring())
    end

  fun ref set_joining(is_joining: Bool) =>
    _is_joining = is_joining


  /////////
  // GLOBAL
  /////////
  fun ref add_worker(worker: WorkerName) =>
    _global_registry.add_worker(worker)

  fun ref remove_worker(worker: WorkerName) =>
    _global_registry.remove_worker(worker)

  fun ref purge_pending_requests(session_id: RoutingId) =>
    _global_registry.purge_pending_requests(session_id)

  /////////////
  // LOCAL
  /////////////

  fun ref stream_notify(request_id: ConnectorStreamNotifyId,
    stream_id: StreamId, stream_name: String,
    point_of_ref: PointOfReference = 0,
    promise: Promise[NotifyResult[In]],
    connector_source: ConnectorSource[In] tag)
  =>
    try
      // stream_id in _active_streams
      let stream = _active_streams(stream_id)?
        // reject: already owned by a source in this registry
        stream_notify_local_result(false, StreamTuple(stream_id, stream_name,
          point_of_ref), promise, connector_source)
    else
      // No local knowledge of stream_id: defer to global
      _global_registry.stream_notify(request_id,
        StreamTuple(stream_id, stream_name, point_of_ref), promise,
        connector_source)
    end

  fun ref stream_notify_local_result(success: Bool,
    stream: StreamTuple,
    promise: Promise[NotifyResult[In]],
    connector_source: ConnectorSource[In] tag)
  =>
    // No local state to update.
    promise(NotifyResult[In](connector_source, success, stream))

  fun ref stream_notify_global_result(success: Bool, stream: StreamTuple,
    promise: Promise[NotifyResult[In]],
    connector_source: ConnectorSource[In] tag)
  =>
    if success then
      // update locally
      _active_streams(stream.id) = ActiveStreamTuple[In](stream.id, stream.name,
        stream.last_acked, stream.last_acked, connector_source)
    end
    promise(NotifyResult[In](connector_source, success, stream))

  fun ref streams_relinquish(streams: Array[StreamTuple] val) =>
    for stream in streams.values() do
      try _active_streams.remove(stream.id)? end
    end
    _global_registry.streams_relinquish(streams)

class val ConnectorStreamNotifyId is Equatable[ConnectorStreamNotifyId]
  let stream_id: StreamId
  let session_id: RoutingId

  new val create(stream_id': StreamId, session_id': RoutingId) =>
    stream_id = stream_id'
    session_id = session_id'

  fun eq(that: ConnectorStreamNotifyId box): Bool =>
     """
    Returns true if the request is for the same session and stream.
    """
    (session_id == that.session_id) and (stream_id == that.stream_id)

  fun hash(): USize =>
    session_id.hash() xor stream_id.hash()
