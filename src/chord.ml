open Core
open Lwt_utils.O

module Messages (Wire : Transport.Wire) = struct
  type query =
    | Successor of Wire.Address.t
    | Hello of {self: (query, response) Wire.peer; predecessor: Wire.Address.t}
    | Get of Wire.Address.t
    | Set of Wire.Address.t * Bytes.t

  and response =
    | Found of (query, response) Wire.peer * (query, response) Wire.peer option
    | Forward of (query, response) Wire.peer
    | Welcome
    | NotTheDroids
    | Done
    | Not_found
    | Value of Bytes.t
end

module MakeDetails
    (W : Transport.Wire)
    (T : Transport.Transport
           with module Messages = Messages(W)
            and module Wire = W) =
struct
  module Address = T.Wire.Address
  module Map = Map.Make (Address)

  type endpoint = T.endpoint

  type state =
    { address: Address.t
    ; predecessor: T.peer
    ; finger: T.peer option Array.t
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
            let open Address.O in
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
    let* resp = T.send transport client (T.Messages.Successor addr) in
    match resp with
    | T.Messages.Found (successor, predecessor) ->
        Lwt.return (successor, predecessor)
    | T.Messages.Forward (_, ep) ->
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
        | T.Messages.Successor addr -> (
            Logs.debug (fun m ->
                m "%a: query: successor %a" pp_node state Address.pp addr) ;
            match finger state addr with
            | None ->
                ( T.Messages.Found
                    ( (state.address, T.endpoint transport server)
                    , Some state.predecessor )
                , state )
            | Some peer ->
                (T.Messages.Forward peer, state) )
        | T.Messages.Hello {self= addr, ep; predecessor} ->
            Logs.debug (fun m ->
                m "%a: query: hello %a (%a)" pp_node state Address.pp addr
                  Address.pp predecessor) ;
            if
              between addr (fst state.predecessor) state.address
              && predecessor = fst state.predecessor
            then (
              Logs.debug (fun m ->
                  m "%a: new predecessor: %a" pp_node state Address.pp addr) ;
              (T.Messages.Welcome, {state with predecessor= (addr, ep)}) )
            else (
              Logs.debug (fun m ->
                  m "%a: unrelated or wrong predecessor: %a" pp_node state
                    Address.pp addr) ;
              (T.Messages.NotTheDroids, state) )
        | T.Messages.Get addr -> (
          match Map.find state.values addr with
          | Some value ->
              Logs.debug (fun m ->
                  m "%a: locally get %a" pp_node state Address.pp addr) ;
              (T.Messages.Value value, state)
          | None ->
              (T.Messages.Not_found, state) )
        | T.Messages.Set (key, data) ->
            if own state key then (
              Logs.debug (fun m ->
                  m "%a: locally set %a" pp_node state Address.pp key) ;
              ( T.Messages.Done
              , {state with values= Map.set state.values ~key ~data} ) )
            else (T.Messages.Not_found, state)
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
        (T.Messages.Hello
           { self= (state.address, T.endpoint transport server)
           ; predecessor= fst state.predecessor })
      >>= function
      | T.Messages.Welcome ->
          Lwt.return true
      | T.Messages.NotTheDroids ->
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
        T.send node.transport client (T.Messages.Get addr)
        >>| function
        | T.Messages.Value v ->
            Result.Ok v
        | T.Messages.Not_found ->
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
        T.send node.transport client (T.Messages.Set (key, data))
        >>| function
        | T.Messages.Done ->
            Result.Ok ()
        | T.Messages.Not_found ->
            Result.Error
              (Format.asprintf "key %a not set remotely on %a" Address.pp key
                 Address.pp peer_addr)
        | _ ->
            failwith "unexpected answer" )
end

module Make
    (W : Transport.Wire)
    (T : Transport.Transport
           with module Messages = Messages(W)
            and module Wire = W) :
  Implementation.Implementation with type Address.t = W.Address.t =
  MakeDetails (W) (T)
