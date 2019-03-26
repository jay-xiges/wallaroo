/*

Copyright 2018 The Wallaroo Authors.

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

use "collections"
use "promises"
use "wallaroo/core/common"
use "wallaroo/core/invariant"
use "wallaroo/core/messages"
use "wallaroo/core/network"
use "wallaroo/core/source"
use "wallaroo/core/source/barrier_source"
use "wallaroo/core/sink"
use "wallaroo_labs/mort"
use "wallaroo_labs/string_set"


actor BarrierInjectorAndCollector
  let _auth: AmbientAuth
  let _worker_name: WorkerName
  // ASSUMPTION: We are currently assuming that the primary worker will never
  // change while a barrier is in flight. This means we don't need to know
  // which primary worker initiated a given barrier since all in flight
  // barriers will have been initiated by the same one. If a new primary
  // worker is elected, the BarrierCoordinator will have to ensure all barriers
  // are done and only then update us here.
  var _primary_worker: WorkerName
  let _coordinator: BarrierCoordinator
  let _connections: Connections

  let _active_barriers: Map[BarrierToken, _SinkAckCount] =
    _active_barriers.create()

  // When barriers are aborted, they're added to this array so that we know
  // to ignore further acks/aborts for the barrier. We only keep the last
  // 256 barriers around.
  let _aborted_barriers: Array[BarrierToken] = Array[BarrierToken](256)

  var _barrier_sources: SetIs[BarrierSource] = _barrier_sources.create()
  let _sources: Map[RoutingId, Source] = _sources.create()
  let _sinks: SetIs[Sink] = _sinks.create()

  var _disposed: Bool = false

  new create(auth: AmbientAuth, worker_name: String,
    primary_worker: WorkerName, coordinator: BarrierCoordinator,
    connections: Connections)
  =>
    _auth = auth
    _worker_name = worker_name
    _primary_worker = primary_worker
    _coordinator = coordinator
    _connections = connections

  be register_sink(sink: Sink) =>
    _sinks.set(sink)

  be unregister_sink(sink: Sink) =>
    _sinks.unset(sink)

  be register_barrier_source(b_source: BarrierSource) =>
    _barrier_sources.set(b_source)

  be register_source(source: Source, source_id: RoutingId) =>
    _sources(source_id) = source

  be unregister_source(source: Source, source_id: RoutingId) =>
    try
      _sources.remove(source_id)?
    else
      Fail()
    end

  be inject_barrier(barrier_token: BarrierToken) =>
    if not _disposed and not _aborted_barriers.contains(barrier_token) then
      ifdef "checkpoint_trace" then
        @printf[I32]("Calling initiate_barrier at %s BarrierSources\n"
          .cstring(), _barrier_sources.size().string().cstring())
      end
      for b_source in _barrier_sources.values() do
        b_source.initiate_barrier(barrier_token)
      end

      ifdef "checkpoint_trace" then
        @printf[I32]("Calling initiate_barrier at %s sources\n".cstring(),
          _sources.size().string().cstring())
      end
      for s in _sources.values() do
        s.initiate_barrier(barrier_token)
      end

      // It's possible we've already initiated this if a sink ack arrived
      // before the inject message.
      if not _active_barriers.contains(barrier_token) then
        _initiate_barrier(barrier_token)
      end
    end

  fun ref _initiate_barrier(token: BarrierToken) =>
    ifdef debug then
      Invariant(not _active_barriers.contains(token))
    end

    let sink_ack_count = _SinkAckCount(token, _sinks, this)
    _active_barriers(token) = sink_ack_count

    sink_ack_count.check_complete()

  be ack_barrier(s: Sink, barrier_token: BarrierToken) =>
    """
    Called by sinks when they have received barrier barriers on all
    their inputs.
    """
    if not _aborted_barriers.contains(barrier_token) then
      // It's possible we haven't initiated this yet if a sink ack arrived
      // before the inject message.
      if not _active_barriers.contains(barrier_token) then
        _initiate_barrier(barrier_token)
      end

      try
        _active_barriers(barrier_token)?.ack(s)
      else
        Fail()
      end
    end

  be abort_barrier(barrier_token: BarrierToken) =>
    """
    Called by a sink that determines a protocol underlying a barrier
    must be aborted.
    """
    if not _aborted_barriers.contains(barrier_token) then
      _abort_barrier(barrier_token)
      if _primary_worker == _worker_name then
        _coordinator.worker_abort_barrier(_worker_name, barrier_token)
      else
        try
          let msg = ChannelMsgEncoder.worker_abort_barrier(_worker_name,
            barrier_token, _auth)?
          _connections.send_control(_primary_worker, msg)
        else
          Fail()
        end
      end
    end

  be remote_abort_barrier(barrier_token: BarrierToken) =>
    """
    Called when another worker has aborted this barrier.
    """
    if not _aborted_barriers.contains(barrier_token) then
      _abort_barrier(barrier_token)
    end

  fun ref _abort_barrier(barrier_token: BarrierToken) =>
      _aborted_barriers.push(barrier_token)
      if _aborted_barriers.size() > 254 then
        try _aborted_barriers.shift()? end
      end
      _clear_barrier(barrier_token)

  fun ref all_sinks_acked(barrier_token: BarrierToken) =>
    """
    Once all sink have acked, we send an ack to the primary BarrierCoordinator.
    """
    if not _disposed then
      _clear_barrier(barrier_token)

      if _primary_worker == _worker_name then
        _coordinator.worker_ack_barrier(_worker_name, barrier_token)
      else
        try
          let msg = ChannelMsgEncoder.worker_ack_barrier(_worker_name,
            barrier_token, _auth)?
          _connections.send_control(_primary_worker, msg)
        else
          Fail()
        end
      end
    end

  fun ref _clear_barrier(token: BarrierToken) =>
    try
      _active_barriers.remove(token)?
    else
      ifdef debug then
        _unknown_barrier_for("_clear_barrier", token)
      end
    end

  be clear_barriers() =>
    _active_barriers.clear()

  be dispose() =>
    @printf[I32]("Shutting down BarrierInjectorAndCollector\n".cstring())
    _disposed = true

  fun ref _unknown_barrier_for(call_name: String, barrier_token: BarrierToken)
  =>
    @printf[I32](("%s received at BarrierCoordinator " +
      "for unknown barrier token %s. Did we rollback?\n").cstring(),
      call_name.cstring(), barrier_token.string().cstring())

class _SinkAckCount
  let _token: BarrierToken
  let _sinks: SetIs[Sink] = _sinks.create()
  let _coordinator: BarrierInjectorAndCollector ref

  new create(t: BarrierToken, sinks: SetIs[Sink],
    lbc: BarrierInjectorAndCollector ref)
  =>
    _token = t
    for s in sinks.values() do
      _sinks.set(s)
    end
    _coordinator = lbc

  fun ref ack(s: Sink) =>
    ifdef debug then
      Invariant(_sinks.contains(s))
    end

    _sinks.unset(s)
    check_complete()

  fun ref check_complete() =>
    if _sinks.size() == 0 then
      _coordinator.all_sinks_acked(_token)
    end

