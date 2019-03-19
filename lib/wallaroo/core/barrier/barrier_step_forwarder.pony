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
use "wallaroo/core/boundary"
use "wallaroo/core/common"
use "wallaroo/core/invariant"
use "wallaroo/core/topology"
use "wallaroo_labs/mort"


class BarrierStepForwarder
  let _step_id: RoutingId
  let _step: Step ref
  var _barrier_token: BarrierToken = InitialBarrierToken
  let _inputs_blocking: Map[RoutingId, Producer] = _inputs_blocking.create()
  let _removed_inputs: SetIs[RoutingId] = _removed_inputs.create()

  new create(step_id: RoutingId, step: Step ref) =>
    _step_id = step_id
    _step = step

  fun ref higher_priority(token: BarrierToken): Bool =>
    token > _barrier_token

  fun ref lower_priority(token: BarrierToken): Bool =>
    token < _barrier_token

  fun barrier_in_progress(): Bool =>
    _barrier_token != InitialBarrierToken

  fun input_blocking(id: RoutingId): Bool =>
    _inputs_blocking.contains(id)

  fun ref receive_new_barrier(step_id: RoutingId, producer: Producer,
    barrier_token: BarrierToken)
  =>
    @printf[I32]("[JB]receive_new_barrier: %s\n".cstring(),
      barrier_token.string().cstring())
    @printf[I32]("[JB]old barrier: %s\n".cstring(),
      _barrier_token.string().cstring())
    _barrier_token = barrier_token
    receive_barrier(step_id, producer, barrier_token)

  fun ref receive_barrier(step_id: RoutingId, producer: Producer,
    barrier_token: BarrierToken)
  =>
    // If this new token is a higher priority token, then the forwarder should
    // have already been cleared to make way for it.
    @printf[I32]("[JB] receive_barrier(step_id: %s barrier_token: %s _barrier_token: %s\n".cstring(),
      step_id.string().cstring(), barrier_token.string().cstring(), _barrier_token.string().cstring())

    ifdef debug then
      @printf[I32]("[JB]is barrier_token > _barrier_token\n".cstring())
      if barrier_token > _barrier_token then
        @printf[I32]("Invariant violation: received barrier %s is greater than current barrier %s at Step %s\n".cstring(), barrier_token.string().cstring(), _barrier_token.string().cstring(), _step_id.string().cstring())
      end
      Invariant(not (barrier_token > _barrier_token))
    end


    // If we're processing a rollback token which is higher priority than
    // this new one, then we need to drop this new one.
    @printf[I32]("[JB]is _barrier_token > barrier_token\n".cstring())
    if _barrier_token > barrier_token then
      return
    end

    @printf[I32]("[JB]is barrier_token != _barrier_token\n".cstring())
    if barrier_token != _barrier_token then
      @printf[I32]("Received %s when still processing %s at step %s\n"
        .cstring(), barrier_token.string().cstring(),
        _barrier_token.string().cstring(), _step_id.string().cstring())
      Fail()
    end

    let inputs = _step.inputs()
    if inputs.contains(step_id) then
      _inputs_blocking(step_id) = producer
      check_completion(inputs)
    else
      if not _removed_inputs.contains(step_id) then
        @printf[I32]("%s: Forwarder at %s doesn't know about %s\n".cstring(), barrier_token.string().cstring(), _step_id.string().cstring(), step_id.string().cstring())
        Fail()
      else
      @printf[I32]("[JB] received_barrier: _removed_inputs does not contain %s\n"
        .cstring(), step_id.string().cstring())
      end
    end

  fun ref remove_input(input_id: RoutingId) =>
    """
    Called if an input leaves the system during barrier processing. This should
    only be possible with Sources that are closed (e.g. when a TCPSource
    connection is dropped).
    """
    if _inputs_blocking.contains(input_id) then
      try
        _inputs_blocking.remove(input_id)?
      else
        Unreachable()
      end
    end
    _removed_inputs.set(input_id)
    if _inputs_blocking.contains(input_id) then
      try _inputs_blocking.remove(input_id)? else Unreachable() end
    end
    check_completion(_step.inputs())

  fun ref check_completion(inputs: Map[RoutingId, Producer] box) =>
    @printf[I32]("[JB] check_completion.1\n".cstring())
    // TODO [source-migration]: John, does the below if do anything?
    if (inputs.size() - _inputs_blocking.size()) == 1 then
      for (kk, xx) in inputs.pairs() do
        var found: Bool = false
        for yy in _inputs_blocking.values() do
          if yy is xx then found = true end
        end
      end
    end
    @printf[I32]("[JB] check_completion.2\n".cstring())
    @printf[I32]("[JB] check_completion.2 _inputs_blocking size: %s\n"
      .cstring(), _inputs_blocking.size().string().cstring())
    if inputs.size() == _inputs_blocking.size()
    then
      for (o_id, o) in _step.outputs().pairs() do
        match o
        | let ob: OutgoingBoundary =>
          ob.forward_barrier(o_id, _step_id,
            _barrier_token)
        else
          o.receive_barrier(_step_id, _step, _barrier_token)
        end
      end
      let b_token = _barrier_token
      clear()
      @printf[I32]("[JB] check_completion.2.clear\n".cstring())
      _step.barrier_complete(b_token)
    end
    @printf[I32]("[JB] check_completion.3\n".cstring())

  fun ref clear() =>
    @printf[I32]("[JB]clear\n".cstring())
    _inputs_blocking.clear()
    _removed_inputs.clear()
    _barrier_token = InitialBarrierToken
