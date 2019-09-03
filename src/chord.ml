open Core
open Lwt_utils.O

module type Transport = sig
  module Address : Implementation.Address

  type t

  val make : unit -> t

  type client

  type server

  type endpoint

  type peer = Address.t * endpoint

  type message =
    | Successor of Address.t
    | Hello of {self: peer; predecessor: Address.t}
    | Get of Address.t
    | Set of Address.t * Bytes.t

  type response =
    | Found of peer * peer option
    | Forward of peer
    | Welcome
    | NotTheDroids
    | Done
    | Not_found
    | Value of Bytes.t

  type id

  val connect : t -> endpoint -> client

  val listen : t -> unit -> server

  val endpoint : t -> server -> endpoint

  val send : t -> client -> message -> response Lwt.t

  val receive : t -> server -> (id * message) Lwt.t

  val respond : t -> server -> id -> response -> unit Lwt.t

  val pp_peer : Format.formatter -> peer -> unit
end

module MakeDetails (T : Transport) = struct
  module Address = T.Address

  type endpoint = T.endpoint

  type peer = T.peer

  module Map = Map.Make (Address)

  type state =
    { address: Address.t
    ; predecessor: peer
    ; finger: peer option Array.t
    ; values: Bytes.t Map.t }

  type node = {mutable state: state; server: T.server; transport: T.t}

  let predecessor node = fst node.state.predecessor

  let pp_node fmt node =
    Format.pp_print_string fmt "node(" ;
    Address.pp fmt node.address ;
    Format.pp_print_string fmt ")"

  let between addr left right =
    let open Address.O in
    if left < right then left < addr && addr <= right
    else not (right < addr && addr <= left)

  let own node addr = between addr (fst node.predecessor) node.address

  let finger state addr =
    let open Address.O in
    if between addr (fst state.predecessor) state.address then (
      Logs.debug (fun m ->
          m "%a: address in our space, response is self" pp_node state) ;
      None )
    else
      let f i = function
        | None ->
            false
        | Some _ ->
            let offset = Address.log (Address.space_log - 1 - i) in
            addr < state.address + offset
      in
      match Core.Array.findi ~f state.finger with
      | Some (_, lookup) ->
          let res = Core.Option.value_exn lookup in
          Logs.debug (fun m ->
              m "%a: response from finger: %a" pp_node state T.pp_peer res) ;
          Some res
      | None ->
          let res = state.predecessor in
          Logs.debug (fun m ->
              m "%a: finger empty, return predecessor: %a" pp_node state
                T.pp_peer res) ;
          Some res

  let rec successor_query transport addr ep =
    let client = T.connect transport ep in
    let* resp = T.send transport client (Successor addr) in
    match resp with
    | T.Found (successor, predecessor) ->
        Lwt.return (successor, predecessor)
    | T.Forward (_, ep) ->
        (successor_query [@tailcall]) transport addr ep
    | _ ->
        Lwt.fail (Failure "unexpected answer")

  let successor node addr =
    match finger node.state addr with
    | None ->
        Lwt.return None
    | Some (_, ep) ->
        let+ succ = successor_query node.transport addr ep in
        Some succ

  let rec make_details ?(transport = T.make ()) address endpoints : node Lwt.t
      =
    Logs.debug (fun m -> m "node(%a): make" Address.pp address) ;
    let server = T.listen transport () in
    let* successor, predecessor =
      match
        Core.List.find_map
          ~f:(fun ep -> Option.some (successor_query transport address ep))
          endpoints
      with
      | None ->
          Logs.warn (fun m ->
              m "node(%a): could not find predecessor" Address.pp address) ;
          Lwt.return (None, None)
      | Some v ->
          let+ successor, predecessor = v in
          Logs.debug (fun m ->
              m "node(%a): found successor: %a" Address.pp address T.pp_peer
                successor) ;
          (Some successor, predecessor)
    in
    let state =
      let self = (address, T.endpoint transport server) in
      { address
      ; predecessor= Core.Option.value predecessor ~default:self
      ; finger= Stdlib.Array.make Address.space_log None
      ; values= Map.empty }
    in
    let res = {server; state; transport} in
    Logs.debug (fun m ->
        m "node(%a): precedessor: %a" Address.pp address T.pp_peer
          state.predecessor) ;
    let rec thread (server, state) =
      let respond = function
        | T.Successor addr -> (
            Logs.debug (fun m ->
                m "%a: query: successor %a" pp_node state Address.pp addr) ;
            match finger state addr with
            | None ->
                ( T.Found
                    ( (state.address, T.endpoint transport server)
                    , Some state.predecessor )
                , state )
            | Some peer ->
                (T.Forward peer, state) )
        | T.Hello {self= addr, ep; predecessor} ->
            Logs.debug (fun m ->
                m "%a: query: hello %a (%a)" pp_node state Address.pp addr
                  Address.pp predecessor) ;
            if
              between addr (fst state.predecessor) state.address
              && predecessor = fst state.predecessor
            then (
              Logs.debug (fun m ->
                  m "%a: new predecessor: %a" pp_node state Address.pp addr) ;
              (T.Welcome, {state with predecessor= (addr, ep)}) )
            else (
              Logs.debug (fun m ->
                  m "%a: unrelated or wrong predecessor: %a" pp_node state
                    Address.pp addr) ;
              (T.NotTheDroids, state) )
        | T.Get addr -> (
          match Map.find state.values addr with
          | Some value ->
              Logs.debug (fun m ->
                  m "%a: locally get %a" pp_node state Address.pp addr) ;
              (T.Value value, state)
          | None ->
              (T.Not_found, state) )
        | T.Set (key, data) ->
            if own state key then (
              Logs.debug (fun m ->
                  m "%a: locally set %a" pp_node state Address.pp key) ;
              (T.Done, {state with values= Map.set state.values ~key ~data}) )
            else (T.Not_found, state)
      in
      let* id, query = T.receive transport server in
      let response, state = respond query in
      res.state <- state ;
      let* () = T.respond transport server id response in
      (thread [@tailcall]) (server, state)
    in
    let hello peer =
      T.send transport
        (T.connect transport (snd peer))
        (T.Hello
           { self= (state.address, T.endpoint transport server)
           ; predecessor= fst state.predecessor })
      >>= function
      | T.Welcome ->
          Lwt.return true
      | T.NotTheDroids ->
          Lwt.return false
      | _ ->
          Lwt.fail (Failure "unexpected answer")
    in
    let* joined =
      match Core.Option.map successor ~f:hello with
      | Some v ->
          v
      | _ ->
          Lwt.return true
    in
    if joined then (
      Logs.debug (fun m -> m "%a: joined successfully" pp_node state) ;
      ignore (thread (server, state)) ;
      Lwt.return res )
    else (
      (* FIXME: no need to rerun EVERYTHING *)
      Logs.debug (fun m ->
          m "%a: wrong successor or predecessor, restarting" pp_node state) ;
      (make_details [@tailcall]) ~transport address endpoints )

  let make = make_details ~transport:(T.make ())

  let endpoint node = T.endpoint node.transport node.server

  let get node addr =
    successor node addr
    >>= function
    | None -> (
      match Map.find node.state.values addr with
      | Some value ->
          Lwt.return (Result.Ok value)
      | None ->
          Lwt.return
            (Result.Error
               (Format.asprintf "key %a not found locally" Address.pp addr)) )
    | Some ((peer_addr, ep), _) -> (
        let client = T.connect node.transport ep in
        T.send node.transport client (T.Get addr)
        >>| function
        | T.Value v ->
            Result.Ok v
        | T.Not_found ->
            Result.Error
              (Format.asprintf "key %a not found remotely on %a" Address.pp
                 addr Address.pp peer_addr)
        | _ ->
            failwith "unexpected answer" )

  let set node key data =
    successor node key
    >>= function
    | None ->
        ignore (Map.set node.state.values ~key ~data) ;
        Lwt.return (Result.Ok ()) (* FIXME  *)
    | Some ((peer_addr, ep), _) -> (
        let client = T.connect node.transport ep in
        T.send node.transport client (T.Set (key, data))
        >>| function
        | T.Done ->
            Result.Ok ()
        | T.Not_found ->
            Result.Error
              (Format.asprintf "key %a not set remotely on %a" Address.pp key
                 Address.pp peer_addr)
        | _ ->
            failwith "unexpected answer" )
end

module Make (T : Transport) :
  Implementation.Implementation with type Address.t = T.Address.t =
  MakeDetails (T)

module DirectTransport (A : Implementation.Address) :
  Transport with module Address = A and type t = unit and type id = unit =
struct
  module Address = A

  type t = unit

  type id = unit

  let make () = ()

  type message =
    | Successor of Address.t
    | Hello of {self: peer; predecessor: Address.t}
    | Get of Address.t
    | Set of Address.t * Bytes.t

  and response =
    | Found of peer * peer option
    | Forward of peer
    | Welcome
    | NotTheDroids
    | Done
    | Not_found
    | Value of Bytes.t

  and server = (message, response) Lwt_utils.RPC.t

  and client = server

  and endpoint = server

  and peer = Address.t * endpoint

  let connect _ e = e

  let listen _ () = Lwt_utils.RPC.make ()

  let endpoint _ s = s

  let send _ = Lwt_utils.RPC.send

  let receive _ rpc = Lwt_utils.RPC.receive rpc >>| fun msg -> ((), msg)

  let respond _ rpc () resp = Lwt_utils.RPC.respond rpc resp

  let pp_peer fmt (addr, _) = Address.pp fmt addr
end
