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

  type response =
    | Found of peer * peer option
    | Forward of peer
    | Welcome
    | NotTheDroids

  val connect : t -> endpoint -> client

  val listen : t -> unit -> server

  val endpoint : t -> server -> endpoint

  val send : t -> client -> message -> response

  val receive : t -> server -> message

  val respond : t -> server -> response -> unit

  val pp_peer : Format.formatter -> peer -> unit
end

module MakeDetails (T : Transport) = struct
  module Address = T.Address

  type endpoint = T.endpoint

  type peer = T.peer

  type state =
    {address: Address.t; predecessor: peer; finger: peer option Array.t}

  type node =
    {state: state; server: T.server; server_thread: Thread.t; transport: T.t}

  let predecessor node = fst node.state.predecessor

  let pp_node fmt node =
    Format.pp_print_string fmt "node(" ;
    Address.pp fmt node.address ;
    Format.pp_print_string fmt ")"

  let between addr left right =
    let open Address.O in
    if left < right then left < addr && addr <= right
    else not (right < addr && addr <= left)

  let rec make_details ?(transport = T.make ()) address endpoints =
    Logs.debug (fun m -> m "node(%a): make" Address.pp address) ;
    let server = T.listen transport () in
    let rec find_successor ep =
      let client = T.connect transport ep in
      match T.send transport client (Successor address) with
      | Found (successor, predecessor) ->
          Some (successor, predecessor)
      | Forward (_, ep) ->
          (find_successor [@tailcall]) ep
      | _ ->
          failwith "unexpected answer"
    in
    let successor, predecessor =
      match Core.List.find_map ~f:find_successor endpoints with
      | None ->
          Logs.warn (fun m ->
              m "node(%a): could not find predecessor" Address.pp address) ;
          (None, None)
      | Some (successor, predecessor) ->
          Logs.debug (fun m ->
              m "node(%a): found successor: %a" Address.pp address T.pp_peer
                successor) ;
          (Some successor, predecessor)
    in
    let state =
      let self = (address, T.endpoint transport server) in
      { address
      ; predecessor= Core.Option.value predecessor ~default:self
      ; finger= Array.make Address.space_log None }
    in
    Logs.debug (fun m ->
        m "node(%a): precedessor: %a" Address.pp address T.pp_peer
          state.predecessor) ;
    let rec thread (server, state) =
      let open Address.O in
      let respond = function
        | T.Successor addr -> (
            Logs.debug (fun m ->
                m "%a: query: successor %a" pp_node state Address.pp addr) ;
            let self =
              T.Found
                ( (state.address, T.endpoint transport server)
                , Some state.predecessor )
            in
            if between addr (fst state.predecessor) state.address then (
              Logs.debug (fun m ->
                  m "%a: address in our space, response is self" pp_node state) ;
              (self, state) )
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
                      m "%a: response from finger: %a" pp_node state T.pp_peer
                        res) ;
                  (T.Forward res, state)
              | None ->
                  let res = state.predecessor in
                  Logs.debug (fun m ->
                      m "%a: finger empty, return predecessor: %a" pp_node
                        state T.pp_peer res) ;
                  (T.Forward res, state) )
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
                  m "%a: unrelated: %a" pp_node state Address.pp addr) ;
              (T.NotTheDroids, state) )
      in
      let query = T.receive transport server in
      let response, state = respond query in
      T.respond transport server response ;
      (thread [@tailcall]) (server, state)
    in
    let hello peer =
      match
        T.send transport
          (T.connect transport (snd peer))
          (T.Hello
             { self= (state.address, T.endpoint transport server)
             ; predecessor= fst state.predecessor })
      with
      | T.Welcome ->
          true
      | T.NotTheDroids ->
          false
      | _ ->
          failwith "unexpected answer"
    in
    match Core.Option.map successor ~f:hello with
    | Some false ->
        (* FIXME: no need to rerun EVERYTHING *)
        Logs.debug (fun m ->
            m "%a: wrong successor or predecessor, restarting" pp_node state) ;
        (make_details [@tailcall]) ~transport address endpoints
    | _ ->
        Logs.debug (fun m -> m "%a: joined successfully" pp_node state) ;
        { state
        ; server
        ; server_thread= Thread.create thread (server, state)
        ; transport }

  let make = make_details ~transport:(T.make ())

  let endpoint node = T.endpoint node.transport node.server
end

module Make (T : Transport) :
  Implementation.Implementation with type Address.t = T.Address.t =
  MakeDetails (T)

module DirectTransport (A : Implementation.Address) = struct
  module Address = A

  type t = unit

  let make () = ()

  type message =
    | Successor of Address.t
    | Hello of {self: peer; predecessor: Address.t}

  and response =
    | Found of peer * peer option
    | Forward of peer
    | Welcome
    | NotTheDroids

  and server = message Event.channel * response Event.channel

  and client = server

  and endpoint = server

  and peer = Address.t * endpoint

  let connect _ e = e

  let listen _ () = (Event.new_channel (), Event.new_channel ())

  let endpoint _ s = s

  let send _ c m =
    Event.sync (Event.send (fst c) m) ;
    Event.sync (Event.receive (snd c))

  let receive _ c = Event.sync (Event.receive (fst c))

  let respond _ c r = Event.sync (Event.send (snd c) r)

  let pp_peer fmt (addr, _) = Address.pp fmt addr
end
