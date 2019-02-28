/*

Copyright 2019 The Wallaroo Authors.

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
use "net"
use "wallaroo/core/network"
use "wallaroo_labs/bytes"
use cp = "wallaroo_labs/connector_protocol"
use "wallaroo_labs/mort"

class ConnectorSinkNotify
  var _fsm_state: cp.ConnectorProtoFsmState = cp.ConnectorProtoFsmDisconnected
  var _header: Bool = true
  var _throttled: Bool = true
  let _stream_id: cp.StreamId = 1
  // SLF TODO: what is our worker name?
  // SLF TODO: what is our RouterId?
  let _stream_name: String = "worker-QQQ-id-QQQ"
  var credits: U32 = 0
  // SLF TODO: How do we get our initial point-of-reference from EventLog?
  var _point_of_ref: cp.MessageId = 0
  var _message_id: cp.MessageId = _point_of_ref
  // 2PC
  var _rtag: U64 = 77777

  fun ref accepted(conn: WallarooOutgoingNetworkActor ref) =>
    Unreachable()

  fun ref auth_failed(conn: WallarooOutgoingNetworkActor ref) =>
    Unreachable()

  fun ref connecting(conn: WallarooOutgoingNetworkActor ref, count: U32) =>
    None

  fun ref connected(conn: WallarooOutgoingNetworkActor ref) =>
    @printf[I32]("ConnectorSink connected\n".cstring())
    _header = true
    _throttled = false
    conn.expect(4)

    // SLF: TODO: configure version string
    // SLF: TODO: configure cookie string
    // SLF: TODO: configure program string
    // SLF: TODO: configure instance_name string
    let hello = cp.HelloMsg("v0.0.1", "Dragons Love Tacos", "a program", "an instance")
    _send_msg(conn, hello)

    // 2PC: We don't know how many transactions the sink has that
    // have been waiting for a phase 2 message.  We need to discover
    // their txn_id strings and abort them.
    let list_u = make_2pc_list_uncommitted()
    try
      let list_u_msg =
        cp.MessageMsg(0, cp.Ephemeral(), 0, 0, None, [list_u])?
      _send_msg(conn, list_u_msg)
    else
      Fail()
    end

    // 2PC: We also don't know how much fine-grained control the sink
    // has for selectively aborting & committing the stuff that we
    // send to it.  Thus, we should not send any Wallaroo app messages
    // to the sink until we get a ReplyUncommittedMsg response.

    _fsm_state = cp.ConnectorProtoFsmHandshake

  fun ref closed(conn: WallarooOutgoingNetworkActor ref) =>
    @printf[I32]("ConnectorSink connection closed\n".cstring())

  fun ref dispose() =>
    @printf[I32]("ConnectorSink connection dispose\n".cstring())

  fun ref connect_failed(conn: WallarooOutgoingNetworkActor ref) =>
    @printf[I32]("ConnectorSink connection failed\n".cstring())

  fun ref expect(conn: WallarooOutgoingNetworkActor ref, qty: USize): USize =>
    qty

  fun ref received(conn: WallarooOutgoingNetworkActor ref, data: Array[U8] iso,
    times: USize): Bool
  =>
    if _header then
      try
        let payload_size: USize = _payload_length(consume data)?

        @printf[I32]("QQQ: ConnectorSink got header\n".cstring())
        conn.expect(payload_size)
        _header = false
      else
        Fail()
      end
      true
    else
      conn.expect(4)
      _header = true
      let data' = recover val consume data end
      @printf[I32]("QQQ: ConnectorSink got body: %s\n".cstring(), _print_array[U8](data').cstring())
      try
        _process_connector_sink_v2_data(conn, data')?
      else
        Fail()
      end
      true
    end

  fun ref sent(conn: WallarooOutgoingNetworkActor ref, data: (String val | Array[U8 val] val))
    : (String val | Array[U8 val] val)
  =>
    Unreachable()
    data

  fun ref sentv(conn: WallarooOutgoingNetworkActor ref,
    data: ByteSeqIter): ByteSeqIter
  =>
    @printf[I32]("Sink sentv\n".cstring())
    for x in data.values() do
      @printf[I32]("Sink sentv: %s\n".cstring(), _print_array[U8](x).cstring())
    end
    data

  fun ref throttled(conn: WallarooOutgoingNetworkActor ref) =>
    @printf[I32]("ConnectorSink is experiencing back pressure\n".cstring())
    _throttled = true

  fun ref unthrottled(conn: WallarooOutgoingNetworkActor ref) =>
    @printf[I32](("ConnectorSink is no longer experiencing" +
      " back pressure\n").cstring())
    _throttled = false

  fun _send_msg(conn: WallarooOutgoingNetworkActor ref, msg: cp.Message) =>
    let w1: Writer = w1.create()
    let w2: Writer = w2.create()

    let b = cp.Frame.encode(msg, w1)
    w2.u32_be(b.size().u32())
    @printf[I32]("Sink b1: size %d\n".cstring(), b.size())
    w2.write(b)

    let b2 = recover trn w2.done() end
    try (conn as ConnectorSink ref)._writev(consume b2, None) else Fail() end

  fun ref _process_connector_sink_v2_data(
    conn: WallarooOutgoingNetworkActor ref, data: Array[U8] val): None ?
  =>
    match cp.Frame.decode(data)?
    | let m: cp.HelloMsg =>
      Fail()
    | let m: cp.OkMsg =>
      if _fsm_state is cp.ConnectorProtoFsmHandshake then
        _fsm_state = cp.ConnectorProtoFsmStreaming

        credits = m.initial_credits
        if credits < 2 then
          _error_and_close(conn, "HEY, too few credits: " + credits.string())
        else
          let notify = cp.NotifyMsg(_stream_id, _stream_name, _message_id)
          _send_msg(conn, notify)
          credits = credits - 1
        end
      else
        _error_and_close(conn, "Bad FSM State: A" + _fsm_state().string())
      end
    | let m: cp.ErrorMsg =>
      _error_and_close(conn, "Bad FSM State: B" + _fsm_state().string())
    | let m: cp.NotifyMsg =>
      _error_and_close(conn, "Bad FSM State: C" + _fsm_state().string())
    | let m: cp.NotifyAckMsg =>
      if _fsm_state is cp.ConnectorProtoFsmStreaming then
        @printf[I32]("SLF TODO: NotifyAck: success %s stream_id %d p-o-r %llu\n".cstring(), m.success.string().cstring(), m.stream_id, m.point_of_ref)
        // TODO: Remove this below, assuming that we always know best?
        if m.point_of_ref > 0 then
          _point_of_ref = m.point_of_ref
          _message_id = _point_of_ref
        end
      else
        _error_and_close(conn, "Bad FSM State: D" + _fsm_state().string())
      end
    | let m: cp.MessageMsg =>
      // 2PC messages are sent via MessageMsg on stream_id 0.
      if (m.stream_id != 0) or (m.message is None) then
        _error_and_close(conn, "Bad FSM State: Ea" + _fsm_state().string())
        return
      end
      @printf[I32]("2PC: GOT MessageMsg\n".cstring())
      try
        let inner = cp.TwoPCFrame.decode(m.message as Array[U8] val)?
        match inner
        | let mi: cp.ReplyUncommittedMsg =>
          // This is a reply to a ListUncommitted message that we sent
          // perhaps some time ago.  Meanwhile, it's possible that we
          // have already started a new round of 2PC ... so our new
          // round's txn_id may be in the txn_id's list.
          // TODO: Filter out any current txn_id before sending the
          // txn abort messages.
          // TODO: Double-check rtag # for sanity.
          ifdef "trace" then
            @printf[I32]("TRACE: uncommitted txns = %d\n".cstring(),
              mi.txn_ids.size())
            for txn_id in mi.txn_ids.values() do
              @printf[I32]("TRACE: rtag %lu txn_id %s\n".cstring(), mi.rtag,
                txn_id.cstring())
              let abort = make_2pc_phase2(txn_id, false)
              let abort_msg =
                cp.MessageMsg(0, cp.Ephemeral(), 0, 0, None, [abort])?
              _send_msg(conn, abort_msg)
            end
          end

          // TODO: remove this dev/scaffolding hack
          let txn_id = "bogus-txn-0"
          let p1 = make_2pc_phase1(txn_id, [(U64(1), U64(0), U64(50))])
          let p1_msg = cp.MessageMsg(0, cp.Ephemeral(), 0, 0, None, [p1])?
          _send_msg(conn, p1_msg)
          let commit = make_2pc_phase2(txn_id, true)
          let commit_msg =
            cp.MessageMsg(0, cp.Ephemeral(), 0, 0, None, [commit])?
          _send_msg(conn, commit_msg)
          // TODO: END OF remove this dev/scaffolding hack

          @printf[I32]("2PC: aborted %d stale transactions, unmuting upstreams\n".cstring(), mi.txn_ids.size())
          try (conn as ConnectorSink ref)._unmute_upstreams() else Fail() end

        | let mi: cp.TwoPCReplyMsg =>
          // TODO: Double-check txn_id for sanity
          // TODO: If commit, then do stuff
          // TODO: If not commit, then do other stuff
          @printf[I32]("2PC: reply for txn_id %s was %s\n".cstring(), mi.txn_id.cstring(), mi.commit.string().cstring())
        else
          Fail()
        end
      else
        _error_and_close(conn, "Bad FSM State: Eb" + _fsm_state().string())
        return
      end
    | let m: cp.AckMsg =>
      if _fsm_state is cp.ConnectorProtoFsmStreaming then
        @printf[I32]("SLF TODO: Ack: credits %d list size = %d\n".cstring(), m.credits, m.credit_list.size())
        credits = credits + m.credits
        for (s_id, p_o_r) in m.credit_list.values() do
          if s_id == _stream_id then
            _point_of_ref = p_o_r
            @printf[I32]("SLF TODO: Ack: stream-id %llu new point-of-reference %llu\n".cstring(), _stream_id, _point_of_ref)
          end
        end
        // SLF TODO: LEFT OFF HERE, process credit_list
      else
        _error_and_close(conn, "Bad FSM State: F" + _fsm_state().string())
      end
    | let m: cp.RestartMsg =>
      ifdef "trace" then
        @printf[I32]("TRACE: got restart message, closing connection\n".cstring())
      end
      conn.close()
    end

  fun ref make_2pc_list_uncommitted(): Array[U8] val =>
    _rtag = _rtag + 1
    let wb: Writer = wb.create()
    let m = cp.ListUncommittedMsg(_rtag)
    cp.TwoPCFrame.encode(m, wb)

  fun ref make_2pc_phase1(txn_id: String, where_list: cp.WhereList):
    Array[U8] val
  =>
    let wb: Writer = wb.create()
    let m = cp.TwoPCPhase1Msg(txn_id, where_list)
    cp.TwoPCFrame.encode(m, wb)

  fun ref make_2pc_phase2(txn_id: String, commit: Bool): Array[U8] val =>
    let wb: Writer = wb.create()
    let m = cp.TwoPCPhase2Msg(txn_id, commit)
    cp.TwoPCFrame.encode(m, wb)

  fun ref _error_and_close(conn: WallarooOutgoingNetworkActor ref,
    msg: String)
  =>
    _send_msg(conn, cp.ErrorMsg(msg))
    conn.close()

  fun ref make_message(encoded1: Array[(String val | Array[U8 val] val)] val):
    cp.MessageMsg ?
  =>
    let stream_id: cp.StreamId = 1
    let flags: cp.Flags = 0
    let event_time = None
    let key = None

    for e in encoded1.values() do
      _message_id = _message_id + e.size().u64()
    end
    cp.MessageMsg(stream_id, flags, _message_id, event_time, key, encoded1)?

  fun _payload_length(data: Array[U8] iso): USize ? =>
    Bytes.to_u32(data(0)?, data(1)?, data(2)?, data(3)?).usize()


  fun _print_array[A: Stringable #read](array: ReadSeq[A]): String =>
    """
    Generate a printable string of the contents of the given readseq to use in
    error messages.
    """
    "[len=" + array.size().string() + ": " + ", ".join(array.values()) + "]"
