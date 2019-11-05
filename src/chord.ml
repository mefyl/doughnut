open Core
open Sexplib

module Result_o = struct
  let ( let* ) = Result.( >>= )

  let ( let+ ) = Result.( >>| )

  let ( and+ ) a b =
    Result.bind a ~f:(fun a -> Result.bind b ~f:(fun b -> Result.Ok (a, b)))
end

module Messages (A : Implementation.Address) (Wire : Transport.Wire) = struct
  module Address = A
  module Wire = Wire

  type peer = {address: Address.t; endpoint: Wire.endpoint}

  let peer_of_sexp =
    let open Result_o in
    function
    | Sexp.List [addr; ep] ->
        let* address = Address.of_sexp addr in
        let+ endpoint = Wire.endpoint_of_sexp ep in
        {address; endpoint}
    | sexp ->
        Result.Error (Format.asprintf "invalid peer: %a" Sexp.pp sexp)

  let sexp_of_peer {address; endpoint} =
    Sexp.List [Address.sexp_of address; Wire.sexp_of_endpoint endpoint]

  let pp_peer fmt {address; endpoint} =
    Format.fprintf fmt "peer %a (%a)" Address.pp address Wire.pp_endpoint
      endpoint

  type query =
    | Successor of Address.t
    | Hello of {self: peer; predecessor: Address.t}
    | Get of Address.t
    | Set of Address.t * Bytes.t

  and response =
    | Found of peer * peer option
    | Forward of {forward: peer; predecessor: peer}
    | Welcome
    | NotTheDroids
    | Done
    | Not_found
    | Value of Bytes.t

  and info = Invalidate of {address: Address.t; actual: peer}

  let sexp_of_query = function
    | Successor addr ->
        Sexp.List [Sexp.Atom "successor"; Address.sexp_of addr]
    | Hello {self; predecessor} ->
        Sexp.List
          [Sexp.Atom "hello"; sexp_of_peer self; Address.sexp_of predecessor]
    | Get key ->
        Sexp.List [Sexp.Atom "get"; Address.sexp_of key]
    | Set (key, value) ->
        Sexp.List
          [ Sexp.Atom "set"
          ; Address.sexp_of key
          ; Sexp.Atom (Bytes.to_string value) ]

  let query_of_sexp =
    let open Result_o in
    function
    | Sexp.List [Sexp.Atom "successor"; addr] ->
        let+ addr = Address.of_sexp addr in
        Successor addr
    | Sexp.List [Sexp.Atom "hello"; self; predecessor] ->
        let* self = peer_of_sexp self
        and+ predecessor = Address.of_sexp predecessor in
        Result.Ok (Hello {self; predecessor})
    | Sexp.List [Sexp.Atom "get"; key] ->
        let+ key = Address.of_sexp key in
        Get key
    | Sexp.List [Sexp.Atom "set"; key; Sexp.Atom value] ->
        let+ key = Address.of_sexp key in
        Set (key, Bytes.of_string value)
    | invalid ->
        Result.Error (Format.asprintf "invalid query: %a" Sexp.pp invalid)

  let sexp_of_response = function
    | Found (peer, pred) ->
        let tail =
          Option.value ~default:[]
            (Option.map ~f:(fun p -> [sexp_of_peer p]) pred)
        in
        Sexp.List (Sexp.Atom "found" :: sexp_of_peer peer :: tail)
    | Forward {forward; predecessor} ->
        Sexp.List
          [Sexp.Atom "forward"; sexp_of_peer forward; sexp_of_peer predecessor]
    | Welcome ->
        Sexp.List [Sexp.Atom "welcome"]
    | NotTheDroids ->
        Sexp.List [Sexp.Atom "not-the-droids"]
    | Done ->
        Sexp.List [Sexp.Atom "done"]
    | Not_found ->
        Sexp.List [Sexp.Atom "not-found"]
    | Value b ->
        Sexp.List [Sexp.Atom "value"; Sexp.Atom (Bytes.to_string b)]

  let response_of_sexp =
    let open Result_o in
    function
    | Sexp.List (Sexp.Atom "found" :: peer :: tail) when List.length tail <= 1
      ->
        let* pred =
          match tail with
          | [] ->
              Result.Ok None
          | [v] ->
              let+ pred = peer_of_sexp v in
              Some pred
          | _ ->
              failwith "unreachable"
        and+ peer = peer_of_sexp peer in
        Result.Ok (Found (peer, pred))
    | Sexp.List [Sexp.Atom "forward"; forward; predecessor] ->
        let* forward = peer_of_sexp forward
        and+ predecessor = peer_of_sexp predecessor in
        Result.Ok (Forward {forward; predecessor})
    | Sexp.List [Sexp.Atom "welcome"] ->
        Result.Ok Welcome
    | Sexp.List [Sexp.Atom "not-the-droids"] ->
        Result.Ok NotTheDroids
    | Sexp.List [Sexp.Atom "done"] ->
        Result.Ok Done
    | Sexp.List [Sexp.Atom "not-found"] ->
        Result.Ok Not_found
    | Sexp.List [Sexp.Atom "value"; Sexp.Atom s] ->
        Result.Ok (Value (Bytes.of_string s))
    | sexp ->
        Result.Error (Format.asprintf "invalid response: %a" Sexp.pp sexp)

  let sexp_of_info = function
    | Invalidate {address; actual} ->
        Sexp.List
          [Sexp.Atom "invalidate"; Address.sexp_of address; sexp_of_peer actual]

  let info_of_sexp =
    let open Result_o in
    function
    | Sexp.List [Sexp.Atom "invalidate"; address; actual] ->
        let* address = Address.of_sexp address
        and+ actual = peer_of_sexp actual in
        Result.Ok (Invalidate {address; actual})
    | sexp ->
        Result.Error (Format.asprintf "invalid response: %a" Sexp.pp sexp)
end

module MakeDetails
    (A : Implementation.Address)
    (W : Transport.Wire)
    (T : Transport.Transport
           with module Wire = W
            and type Messages.query = Messages(A)(W).query
            and type Messages.response = Messages(A)(W).response
            and type Messages.info = Messages(A)(W).info) =
struct
  module Address = A
  module Messages = Messages (A) (W)
  module Transport = T
  module Map = Stdlib.Map.Make (Address)

  type endpoint = Transport.endpoint

  type finger = Messages.peer option Array.t

  type state =
    { address: Address.t
    ; predecessor: Messages.peer
    ; finger: finger
    ; values: Bytes.t Map.t }

  type node =
    {mutable state: state; server: Transport.server; transport: Transport.t}

  let predecessor node = node.state.predecessor.address

  let pp_node fmt node =
    Format.pp_print_string fmt "node(" ;
    Address.pp fmt node.address ;
    Format.pp_print_string fmt ")"

  let between addr left right =
    let open Address.O in
    if left < right then left < addr && addr <= right
    else not (right < addr && addr <= left)

  let own node addr = between addr node.predecessor.address node.address

  let finger state addr =
    if between addr state.predecessor.address state.address then (
      Logs.debug (fun m ->
          m "%a: address %a in our space, response is self" pp_node state
            Address.pp addr) ;
      None )
    else
      let f i = function
        | None ->
            false
        | Some (peer : Messages.peer) ->
            i = Stdlib.Array.length state.finger - 1
            ||
            let open Address.O in
            peer.address - state.address < addr - state.address
      in
      match Core.Array.findi ~f state.finger with
      | Some (_, lookup) ->
          let res = Core.Option.value_exn lookup in
          Logs.debug (fun m ->
              m "%a: finger response for %a: %a" pp_node state Address.pp addr
                Messages.pp_peer res) ;
          Some res
      | None ->
          Logs.err (fun m -> m "ouhlala") ;
          failwith "ouhlala"

  let successor_query transport state addr ep =
    Logs.debug (fun m -> m "%a: lookup (%a)" pp_node state Address.pp addr) ;
    let query ep =
      let client = Transport.connect transport ep in
      Transport.send transport client (Messages.Successor addr)
    in
    let open Lwt_utils.O_result in
    let rec finalize (initiator : Messages.peer) ?(failed = false)
        (peer : Messages.peer) =
      query peer.endpoint
      >>= function
      | Messages.Found (successor, predecessor) ->
          if failed then (
            let client = Transport.connect transport initiator.endpoint in
            Logs.debug (fun m ->
                m "%a: invalidate %a on %a to %a" pp_node state Address.pp addr
                  Messages.pp_peer initiator Messages.pp_peer successor) ;
            Transport.inform transport client
              (Messages.Invalidate {address= addr; actual= successor}) ) ;
          Lwt_result.return (successor, predecessor)
      | Messages.Forward {predecessor; _} ->
          Logs.debug (fun m ->
              m "%a: refusing to forward lookup (%a) past %a, backtrack to %a"
                pp_node state Address.pp addr Messages.pp_peer peer
                Messages.pp_peer predecessor) ;
          (finalize [@tailcall]) initiator ~failed:true predecessor
      | _ ->
          Lwt.fail (Failure "unexpected answer")
    in
    let rec iter (peer, ep) =
      query ep
      >>= function
      | Messages.Found (successor, predecessor) ->
          Lwt_result.return (successor, predecessor)
      | Messages.Forward {forward; _}
        when Option.value_map peer ~default:false ~f:(fun peer ->
                 between addr peer forward.address) ->
          Logs.debug (fun m ->
              m "%a: forward final lookup (%a) to %a" pp_node state Address.pp
                addr Messages.pp_peer forward) ;
          (finalize [@tailcall])
            {address= Option.value_exn peer; endpoint= ep}
            forward
      | Messages.Forward {forward; _} ->
          Logs.debug (fun m ->
              m "%a: forward lookup (%a) to %a" pp_node state Address.pp addr
                Messages.pp_peer forward) ;
          (iter [@tailcall]) (Some forward.address, forward.endpoint)
      | _ ->
          Lwt.fail (Failure "unexpected answer")
    in
    iter (None, ep)

  let successor transport state addr =
    let open Lwt_utils.O_result in
    match finger state addr with
    | None ->
        Lwt_result.return None
    | Some {endpoint; _} ->
        let+ succ = successor_query transport state addr endpoint in
        Some succ

  let finger_add state finger ({Messages.address; _} as peer) =
    let finger = Stdlib.Array.copy finger in
    let rec fill n =
      let index = Address.space_log - n - 1
      and pivot =
        let open Address.O in
        state.address + Address.log n
      in
      let current = Stdlib.Array.get finger index in
      if
        Option.value ~default:true
          (Option.map current ~f:(fun {Messages.address= c; _} ->
               between address state.address c))
      then
        if between pivot state.address address then (
          Logs.debug (fun m ->
              m "%a: update finger entry %i (%a) to %a" pp_node state n
                Address.pp pivot Messages.pp_peer peer) ;
          Stdlib.Array.set finger index (Some peer) ) ;
      if n < Address.space_log - 1 then (fill [@tailcall]) (n + 1)
    in
    fill 0 ; finger

  let finger_check transport state =
    Logs.debug (fun m -> m "%a: refresh finger table" pp_node state) ;
    let rec check state n =
      assert (n >= 0 && n < Address.space_log) ;
      let index = Address.space_log - n - 1 in
      let pivot =
        let open Address.O in
        state.address + Address.log n
      in
      let open Lwt_utils.O_result in
      let* state =
        successor transport state pivot
        >>| function
        | None ->
            Logs.debug (fun m ->
                m "%a: finger entry %i left empty" pp_node state n) ;
            state
        | Some (succ, _) -> (
          match Stdlib.Array.get state.finger index with
          | Some v when Address.compare v.address succ.address = 0 ->
              Logs.debug (fun m ->
                  m "%a: leave finger entry %i to %a" pp_node state n
                    Messages.pp_peer succ) ;
              state
          | _ ->
              Logs.debug (fun m ->
                  m "%a: update finger entry %i (%a) to %a" pp_node state n
                    Address.pp pivot Messages.pp_peer succ) ;
              let finger = Stdlib.Array.copy state.finger in
              Stdlib.Array.set finger index (Some succ) ;
              {state with finger} )
      in
      if n = Address.space_log - 1 then Lwt_result.return state
      else (check [@tailcall]) state (n + 1)
    in
    check state 0

  let rec make_details ?(transport = Transport.make ()) address endpoints =
    Logs.debug (fun m -> m "node(%a): make" Address.pp address) ;
    let server = Transport.listen transport () in
    let open Lwt_utils.O_result in
    let* successor, predecessor =
      match
        Core.List.find_map
          ~f:(fun endpoint ->
            Option.some
              (successor_query transport
                 { address
                 ; predecessor= {address= Address.null; endpoint}
                 ; finger= Stdlib.Array.make 0 None
                 ; values= Map.empty }
                 address endpoint))
          endpoints
      with
      | None ->
          Logs.warn (fun m ->
              m "node(%a): could not find predecessor" Address.pp address) ;
          Lwt_result.return (None, None)
      | Some v ->
          let+ successor, predecessor = v in
          Logs.debug (fun m ->
              m "node(%a): found successor: %a" Address.pp address
                Messages.pp_peer successor) ;
          (Some successor, predecessor)
    in
    let state =
      let size = Address.space_log in
      let finger = Stdlib.Array.make size None in
      Stdlib.Array.set finger (size - 1) successor ;
      let self : Messages.peer =
        {address; endpoint= Transport.endpoint transport server}
      in
      let predecessor = Core.Option.value predecessor ~default:self in
      let finger = Stdlib.Array.make Address.space_log None in
      {address; predecessor; finger; values= Map.empty}
    in
    let state =
      let finger =
        Option.value ~default:state.finger
          (Option.map successor ~f:(fun n -> finger_add state state.finger n))
      in
      {state with finger}
    in
    let res = {server; state; transport} in
    Logs.debug (fun m ->
        m "node(%a): precedessor: %a" Address.pp address Messages.pp_peer
          state.predecessor) ;
    let rec thread (server, state) =
      let respond = function
        | Messages.Successor addr -> (
            Logs.debug (fun m ->
                m "%a: query: successor %a" pp_node state Address.pp addr) ;
            match finger state addr with
            | None ->
                ( Messages.Found
                    ( { address= state.address
                      ; endpoint= Transport.endpoint transport server }
                    , Some state.predecessor )
                , state )
            | Some peer ->
                ( Messages.Forward
                    {forward= peer; predecessor= state.predecessor}
                , state ) )
        | Messages.Hello {self; predecessor} ->
            Logs.debug (fun m ->
                m "%a: query: hello %a (%a)" pp_node state Address.pp
                  self.address Address.pp predecessor) ;
            if
              between self.address state.predecessor.address state.address
              && predecessor = state.predecessor.address
            then (
              Logs.debug (fun m ->
                  m "%a: new predecessor: %a" pp_node state Address.pp
                    self.address) ;
              ( Messages.Welcome
              , { state with
                  predecessor= self
                ; finger= finger_add state state.finger self } ) )
            else (
              Logs.debug (fun m ->
                  m "%a: unrelated or wrong predecessor: %a" pp_node state
                    Address.pp self.address) ;
              (Messages.NotTheDroids, state) )
        | Messages.Get addr -> (
          match Map.find_opt addr state.values with
          | Some value ->
              Logs.debug (fun m ->
                  m "%a: locally get %a" pp_node state Address.pp addr) ;
              (Messages.Value value, state)
          | None ->
              (Messages.Not_found, state) )
        | Messages.Set (key, data) ->
            if own state key then (
              Logs.debug (fun m ->
                  m "%a: locally set %a" pp_node state Address.pp key) ;
              ( Messages.Done
              , {state with values= Map.add key data state.values} ) )
            else (Messages.Not_found, state)
      in
      let open Lwt_utils.O in
      let* id, query = Transport.receive transport server in
      let response, state = respond query in
      res.state <- state ;
      let* () = Transport.respond transport server id response in
      (thread [@tailcall]) (server, state)
    in
    let hello (peer : Messages.peer) =
      Transport.send transport
        (Transport.connect transport peer.endpoint)
        (Messages.Hello
           { self=
               { address= state.address
               ; endpoint= Transport.endpoint transport server }
           ; predecessor= state.predecessor.address })
      >>= function
      | Messages.Welcome ->
          Lwt_result.return true
      | Messages.NotTheDroids ->
          Lwt_result.return false
      | _ ->
          Lwt_result.fail "unexpected answer"
    in
    let* joined =
      match Core.Option.map successor ~f:hello with
      | Some v ->
          v
      | _ ->
          Lwt_result.return true
    in
    if joined then (
      Logs.debug (fun m -> m "%a: joined successfully" pp_node state) ;
      (* let* state = finger_check transport state in *)
      (* Logs.debug (fun m -> m "%a: finger initialized" pp_node state) ; *)
      (* let res = {res with state} in *)
      let rec response_thread () =
        Lwt.catch
          (fun () -> thread (server, state))
          (fun e ->
            Logs.err (fun m ->
                m "node(%a): fatal error in response thread: %a" Address.pp
                  address Exn.pp e) ;
            response_thread ())
      in
      ignore (response_thread ()) ;
      Lwt_result.return res )
    else (
      (* FIXME: no need to rerun EVERYTHING *)
      Logs.debug (fun m ->
          m "%a: wrong successor or predecessor, restarting" pp_node state) ;
      (make_details [@tailcall]) ~transport address endpoints )

  let make = make_details ~transport:(Transport.make ())

  let endpoint node = Transport.endpoint node.transport node.server

  let get node addr =
    let open Lwt_utils.O_result in
    successor node.transport node.state addr
    >>= function
    | None -> (
      match Map.find_opt addr node.state.values with
      | Some value ->
          Lwt_result.return value
      | None ->
          Lwt_result.fail
            (Format.asprintf "key %a not found locally" Address.pp addr) )
    | Some (peer, _) -> (
        let client = Transport.connect node.transport peer.endpoint in
        Transport.send node.transport client (Messages.Get addr)
        >>= function
        | Messages.Value v ->
            Lwt_result.return v
        | Messages.Not_found ->
            Lwt_result.fail
              (Format.asprintf "key %a not found remotely on %a" Address.pp
                 addr Messages.pp_peer peer)
        | _ ->
            Lwt_result.fail "unexpected answer" )

  let set node key data =
    let open Lwt_utils.O_result in
    successor node.transport node.state key
    >>= function
    | None ->
        node.state <-
          {node.state with values= Map.add key data node.state.values} ;
        Lwt_result.return () (* FIXME *)
    | Some (peer, _) -> (
        let client = Transport.connect node.transport peer.endpoint in
        Transport.send node.transport client (Messages.Set (key, data))
        >>= function
        | Messages.Done ->
            Lwt_result.return ()
        | Messages.Not_found ->
            Lwt_result.fail
              (Format.asprintf "key %a not set remotely on %a" Address.pp key
                 Messages.pp_peer peer)
        | _ ->
            Lwt_result.fail "unexpected answer" )
end

(* module Make (A : Implementation.Address) (W : Transport.Wire) : *)
(*   Implementation.Implementation with module Address = A = *)
(*   MakeDetails (A) (W) (Transport.Make (W) (Messages (A) (W))) *)
