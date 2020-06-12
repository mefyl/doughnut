open Utils
module Format = Caml.Format

let lwt_ok = Lwt.map ~f:Result.return

open Let.Syntax2 (Lwt_result)

module Message (A : Address.S) (Wire : Transport.Wire) = struct
  module Address = A
  module Wire = Wire

  let name = "chord"

  let version = (0, 0, 0)

  type peer = {
    address : Address.t;
    endpoint : Wire.Endpoint.t;
  }

  let peer_of_sexp =
    let open Let.Syntax2 (Result) in
    function
    | Sexp.List [ addr; ep ] ->
      let* address = Address.of_sexp addr in
      let+ endpoint = Wire.Endpoint.of_sexp ep in
      { address; endpoint }
    | sexp -> Result.Error (Format.asprintf "invalid peer: %a" Sexp.pp sexp)

  let sexp_of_peer { address; endpoint } =
    Sexp.List [ Address.sexp_of address; Wire.Endpoint.to_sexp endpoint ]

  let pp_peer fmt { address; endpoint } =
    Format.fprintf fmt "peer %a (%a)" Address.pp address Wire.Endpoint.pp
      endpoint

  open Transport.MessageType

  type _ t =
    | Successor : Address.t -> query t
    | Hello : {
        self : peer;
        predecessor : Address.t;
      }
        -> query t
    | Get : Address.t -> query t
    | Set : Address.t * Bytes.t -> query t
    | Found : peer * peer option -> response t
    | Forward : {
        forward : peer;
        predecessor : peer;
      }
        -> response t
    | Welcome : response t
    | NotTheDroids : response t
    | Done : response t
    | Not_found : response t
    | Value : Bytes.t -> response t
    | Invalidate : {
        address : Address.t;
        actual : peer;
      }
        -> info t

  let sexp_of_message (type m) (msg : m t) =
    match msg with
    | Successor addr ->
      Sexp.List [ Sexp.Atom "successor"; Address.sexp_of addr ]
    | Hello { self; predecessor } ->
      Sexp.List
        [ Sexp.Atom "hello"; sexp_of_peer self; Address.sexp_of predecessor ]
    | Get key -> Sexp.List [ Sexp.Atom "get"; Address.sexp_of key ]
    | Set (key, value) ->
      Sexp.List
        [
          Sexp.Atom "set";
          Address.sexp_of key;
          Sexp.Atom (Bytes.to_string value);
        ]
    | Found (peer, pred) ->
      let tail =
        Option.value ~default:[]
          (Option.map ~f:(fun p -> [ sexp_of_peer p ]) pred)
      in
      Sexp.List (Sexp.Atom "found" :: sexp_of_peer peer :: tail)
    | Forward { forward; predecessor } ->
      Sexp.List
        [ Sexp.Atom "forward"; sexp_of_peer forward; sexp_of_peer predecessor ]
    | Welcome -> Sexp.List [ Sexp.Atom "welcome" ]
    | NotTheDroids -> Sexp.List [ Sexp.Atom "not-the-droids" ]
    | Done -> Sexp.List [ Sexp.Atom "done" ]
    | Not_found -> Sexp.List [ Sexp.Atom "not-found" ]
    | Value b -> Sexp.List [ Sexp.Atom "value"; Sexp.Atom (Bytes.to_string b) ]
    | Invalidate { address; actual } ->
      Sexp.List
        [ Sexp.Atom "invalidate"; Address.sexp_of address; sexp_of_peer actual ]

  let query_of_sexp =
    let open Let.Syntax2 (Result) in
    function
    | Sexp.List [ Sexp.Atom "successor"; addr ] ->
      let+ addr = Address.of_sexp addr in
      Successor addr
    | Sexp.List [ Sexp.Atom "hello"; self; predecessor ] ->
      let* self = peer_of_sexp self
      and+ predecessor = Address.of_sexp predecessor in
      Result.Ok (Hello { self; predecessor })
    | Sexp.List [ Sexp.Atom "get"; key ] ->
      let+ key = Address.of_sexp key in
      Get key
    | Sexp.List [ Sexp.Atom "set"; key; Sexp.Atom value ] ->
      let+ key = Address.of_sexp key in
      Set (key, Bytes.of_string value)
    | invalid ->
      Result.Error (Format.asprintf "invalid query: %a" Sexp.pp invalid)

  let response_of_sexp =
    let open Let.Syntax2 (Result) in
    function
    | Sexp.List (Sexp.Atom "found" :: peer :: tail) when List.length tail <= 1
      ->
      let* pred =
        match tail with
        | [] -> Result.Ok None
        | [ v ] ->
          let+ pred = peer_of_sexp v in
          Some pred
        | _ -> failwith "unreachable"
      and+ peer = peer_of_sexp peer in
      Result.Ok (Found (peer, pred))
    | Sexp.List [ Sexp.Atom "forward"; forward; predecessor ] ->
      let* forward = peer_of_sexp forward
      and+ predecessor = peer_of_sexp predecessor in
      Result.Ok (Forward { forward; predecessor })
    | Sexp.List [ Sexp.Atom "welcome" ] -> Result.Ok Welcome
    | Sexp.List [ Sexp.Atom "not-the-droids" ] -> Result.Ok NotTheDroids
    | Sexp.List [ Sexp.Atom "done" ] -> Result.Ok Done
    | Sexp.List [ Sexp.Atom "not-found" ] -> Result.Ok Not_found
    | Sexp.List [ Sexp.Atom "value"; Sexp.Atom s ] ->
      Result.Ok (Value (Bytes.of_string s))
    | sexp -> Result.Error (Format.asprintf "invalid response: %a" Sexp.pp sexp)

  let info_of_sexp =
    let open Let.Syntax2 (Result) in
    function
    | Sexp.List [ Sexp.Atom "invalidate"; address; actual ] ->
      let* address = Address.of_sexp address
      and+ actual = peer_of_sexp actual in
      Result.Ok (Invalidate { address; actual })
    | sexp -> Result.Error (Format.asprintf "invalid response: %a" Sexp.pp sexp)
end

module MakeDetails
    (A : Address.S)
    (W : Transport.Wire)
    (Transport : Transport.Transport
                   with module Wire = W
                    and module Message.Address = A
                    and type 'a Message.t = 'a Message(A)(W).t) =
struct
  module Address = A
  module Message = Message (A) (W)
  module Wire = W
  module Transport = Transport
  module Endpoint = Transport.Wire.Endpoint
  module Map = Stdlib.Map.Make (Address)

  let between addr left right =
    let open Address.O in
    if left < right then
      left < addr && addr <= right
    else
      not (right < addr && addr <= left)

  module Finger = struct
    type t = {
      address : Address.t;
      index : Message.peer option Array.t;
    }

    let make address successor =
      { address; index = Stdlib.Array.make Address.space_log successor }

    let lookup { address; index } addr =
      let f i = function
        | None -> false
        | Some (peer : Message.peer) ->
          i = Stdlib.Array.length index - 1
          ||
          let open Address.O in
          peer.address - address < addr - address
      in
      Array.findi ~f index

    let pp fmt { index; address } =
      let f =
        let open Fmt in
        const Address.pp address ++ sp ++ const char '@'
        ++ ( brackets
           @@ array ~sep:(const string ", ")
                (option
                   (using (fun { Message.address; _ } -> address) Address.pp))
           )
      in
      f fmt index

    let add { address; index } (peer : Message.peer) =
      let index = Stdlib.Array.copy index in
      let rec fill n =
        let i = Address.space_log - n - 1
        and pivot =
          let open Address.O in
          address + Address.log n
        in
        let current = Stdlib.Array.get index i in
        if
          Option.value ~default:true
            (Option.map current ~f:(fun { Message.address = c; _ } ->
                 between peer.address address c))
        then
          if between pivot address peer.address then (
            Logs.debug (fun m ->
                m "update finger entry %i (%a) to %a" n Address.pp pivot
                  Message.pp_peer peer);
            Stdlib.Array.set index i (Some peer)
          );
        if n < Address.space_log - 1 then (fill [@tailcall]) (n + 1)
      in
      fill 0;
      { address; index }
  end

  type state = {
    address : Address.t;
    endpoint : Endpoint.t;
    predecessor : Message.peer;
    finger : Finger.t;
    values : Bytes.t Map.t;
  }

  type node = state Transport.server

  let state server =
    let get state = Lwt_result.return (state, state) in
    Transport.state server get

  let predecessor node =
    let+ state = state node in
    state.predecessor.address

  let pp_node fmt server =
    Format.fprintf fmt "node(%a)" Address.pp (Transport.address server)

  let own node addr = between addr node.predecessor.address node.address

  let finger server state addr =
    if between addr state.predecessor.address state.address then (
      Logs.debug (fun m ->
          m "%a: address %a in our space, response is self" pp_node server
            Address.pp addr);
      None
    ) else
      match Finger.lookup state.finger addr with
      | Some (_, lookup) ->
        let res = Option.value_exn lookup in
        Logs.debug (fun m ->
            m "%a: finger response for %a: %a" pp_node server Address.pp addr
              Message.pp_peer res);
        Some res
      | None ->
        Logs.err (fun m -> m "ouhlala");
        failwith "ouhlala"

  type internal = Internal

  type external_ = External

  type 'a successor_source =
    | Initial : state * Message.peer -> internal successor_source
    | Forward : Message.peer * Message.peer -> internal successor_source
    | External : Endpoint.t -> external_ successor_source
    | External_forward : Message.peer -> external_ successor_source

  let successor_query (type k) server (source : k successor_source) addr =
    let is_self a =
      match source with
      | Initial (state, _) ->
        Option.some_if (Address.compare state.address a = 0) state
      | _ -> None
    in
    let* () =
      Log.debug (fun m -> m "%a: lookup (%a)" pp_node server Address.pp addr)
    in
    let query ep =
      let* client = Transport.connect server ep in
      Transport.send client (Message.Successor addr)
    in
    let rec finalize source ?(failed = false) (peer : Message.peer) =
      let* () =
        Log.debug (fun m ->
            m "forward final lookup (%a) to %a" Address.pp addr Message.pp_peer
              peer)
      in
      query peer.endpoint >>= function
      | Message.Found (successor, predecessor) ->
        let* state =
          if failed then
            match source with
            | Forward (source, _) ->
              let* client = Transport.connect server source.endpoint in
              let* () =
                Log.debug (fun m ->
                    m "invalidate %a on %a to %a" Address.pp addr
                      Message.pp_peer source Message.pp_peer successor)
              in
              let* () =
                Transport.inform client
                  (Message.Invalidate { address = addr; actual = successor })
              in
              Lwt_result.return None
            | Initial (state, _) ->
              let* () = Log.debug (fun m -> m "heal finger table") in
              let state =
                { state with finger = Finger.add state.finger successor }
              in
              Lwt_result.return (Some state)
          else
            Lwt_result.return None
        in
        Lwt_result.return ((successor, predecessor), state)
      | Message.Forward { predecessor; _ } ->
        let* () =
          Log.debug (fun m ->
              m "refusing to forward lookup (%a) past %a, backtrack to %a"
                Address.pp addr Message.pp_peer peer Message.pp_peer predecessor)
        in
        (finalize [@tailcall]) source ~failed:true predecessor
      | _ -> Lwt.fail (Failure "unexpected answer")
    in
    let rec iter :
        type k.
        k successor_source ->
        ( (Message.peer * Message.peer option) * state option,
          string )
        Lwt_result.t =
     fun source ->
      let query endpoint peer =
        query endpoint >>= function
        | Message.Found (successor, predecessor) ->
          Lwt_result.return ((successor, predecessor), None)
        | Message.Forward { forward; _ } -> (
          match is_self forward.address with
          | Some state ->
            let* () =
              Log.debug (fun m ->
                  m
                    "lookup looped back to ourselves, backtrack to predecessor \
                     %a"
                    Message.pp_peer state.predecessor)
            in
            finalize
              (Forward (Option.value_exn peer, forward))
              ~failed:true state.predecessor
          | None -> (
            let* () =
              Log.debug (fun m ->
                  m "forward lookup (%a) to %a" Address.pp addr Message.pp_peer
                    forward)
            in
            match peer with
            | Some target -> (iter [@tailcall]) (Forward (target, forward))
            | None -> (iter [@tailcall]) (External_forward forward) ) )
        | _ -> Lwt.fail (Failure "unexpected answer")
      in
      match source with
      | Initial (state, target) ->
        if between addr state.address target.address then
          (finalize [@tailcall]) source target
        else
          query target.endpoint (Some target)
      | Forward (prev, target) ->
        if between addr prev.address target.address then
          (finalize [@tailcall]) source target
        else
          query target.endpoint (Some target)
      | External endpoint -> query endpoint None
      | External_forward target -> query target.endpoint (Some target)
    in
    iter source

  let successor server state addr =
    match finger server state addr with
    | None -> Lwt_result.return None
    | Some peer ->
      let+ succ = successor_query server (Initial (state, peer)) addr in
      Some succ

  (* let finger_check transport state =
   *   Logs.debug (fun m -> m "%a: refresh finger table" pp_node state);
   *   let rec check state n =
   *     assert (n >= 0 && n < Address.space_log);
   *     let i = Address.space_log - n - 1 in
   *     let pivot =
   *       let open Address.O in
   *       state.address + Address.log n
   *     in
   *     let* state =
   *       successor transport state pivot >>| function
   *       | None ->
   *         Logs.debug (fun m ->
   *             m "%a: finger entry %i left empty" pp_node state n);
   *         state
   *       | Some ((succ, _), state) -> (
   *         match Stdlib.Array.get state.finger.index i with
   *         | Some v when Address.compare v.address succ.address = 0 ->
   *           Logs.debug (fun m ->
   *               m "%a: leave finger entry %i to %a" pp_node state n
   *                 Message.pp_peer succ);
   *           state
   *         | _ ->
   *           Logs.debug (fun m ->
   *               m "%a: update finger entry %i (%a) to %a" pp_node state n
   *                 Address.pp pivot Message.pp_peer succ);
   *           let index = Stdlib.Array.copy state.finger.index in
   *           Stdlib.Array.set index i (Some succ);
   *           { state with finger = { state.finger with index } } )
   *     in
   *     if n = Address.space_log - 1 then
   *       Lwt_result.return state
   *     else
   *       (check [@tailcall]) state (n + 1)
   *   in
   *   check state 0 *)

  let make_details
      address transport ?(started = fun _ -> Lwt_result.return ()) endpoints =
    let* () =
      Logs_lwt.debug (fun m -> m "node(%a): make" Address.pp address) |> lwt_ok
    in
    let rec init server =
      let endpoint = Transport.endpoint server in
      let* () = started endpoint in
      let* successor, predecessor =
        let f endpoint =
          Option.some (successor_query server (External endpoint) address)
        in
        match List.find_map ~f endpoints with
        | None ->
          Logs.warn (fun m ->
              m "node(%a): could not find predecessor" Address.pp address);
          Lwt_result.return (None, None)
        | Some v ->
          let+ (successor, predecessor), _ = v in
          Logs.debug (fun m ->
              m "node(%a): found successor: %a" Address.pp address
                Message.pp_peer successor);
          (Some successor, predecessor)
      in
      let self : Message.peer = { address; endpoint } in
      let predecessor = Option.value predecessor ~default:self in
      let finger = Finger.make address successor in
      let* () =
        Log.debug (fun m ->
            m "node(%a): predecessor: %a" Address.pp address Message.pp_peer
              predecessor)
      in
      let state =
        { address; predecessor; finger; values = Map.empty; endpoint }
      in
      let* joined =
        let hello (peer : Message.peer) =
          let* client = Transport.connect server peer.endpoint in
          Transport.send client
            (Message.Hello
               {
                 self = { address = state.address; endpoint };
                 predecessor = state.predecessor.address;
               })
          >>= function
          | Message.Welcome -> Lwt_result.return true
          | Message.NotTheDroids -> Lwt_result.return false
          | _ -> Lwt_result.fail "unexpected answer"
        in
        match Option.map successor ~f:hello with
        | Some v -> v
        | _ -> Lwt_result.return true
      in
      if joined then
        let+ () =
          Log.debug (fun m -> m "%a: joined successfully" pp_node server)
        in
        state
      else
        (* FIXME: no need to rerun EVERYTHING *)
        let* () =
          Log.debug (fun m ->
              m "%a: wrong successor or predecessor, restarting" pp_node server)
        in
        (init [@tailcall]) server
    and respond server = function
      | Message.Successor addr ->
        let f state =
          let* () =
            Log.debug (fun m ->
                m "%a: query: successor %a" pp_node server Address.pp addr)
          in
          match finger server state addr with
          | None ->
            Lwt_result.return
              ( state,
                Message.Found
                  ( { address = state.address; endpoint = state.endpoint },
                    Some state.predecessor ) )
          | Some peer ->
            Lwt_result.return
              ( state,
                Message.Forward
                  { forward = peer; predecessor = state.predecessor } )
        in
        Transport.state server f
      | Message.Hello { self; predecessor } ->
        let f state =
          let* () =
            Log.debug (fun m ->
                m "%a: query: hello %a (%a)" pp_node server Address.pp
                  self.address Address.pp predecessor)
          in
          if
            let open Address.O in
            between self.address state.predecessor.address state.address
            && predecessor = state.predecessor.address
          then
            let* () =
              Log.debug (fun m ->
                  m "%a: new predecessor: %a" pp_node server Address.pp
                    self.address)
            in
            let state =
              {
                state with
                predecessor = self;
                finger = Finger.add state.finger self;
              }
            in
            Lwt_result.return (state, Message.Welcome)
          else
            let* () =
              Log.debug (fun m ->
                  m "%a: unrelated or wrong predecessor: %a" pp_node server
                    Address.pp self.address)
            in
            Lwt_result.return (state, Message.NotTheDroids)
        in
        Transport.state server f
      | Message.Get addr ->
        let f state =
          match Map.find_opt addr state.values with
          | Some value ->
            let* () =
              Log.debug (fun m ->
                  m "%a: locally get %a" pp_node server Address.pp addr)
            in
            Lwt_result.return (state, Message.Value value)
          | None -> Lwt_result.return (state, Message.Not_found)
        in
        Transport.state server f
      | Message.Set (key, data) ->
        let f state =
          if own state key then
            let* () =
              Log.debug (fun m ->
                  m "%a: locally set %a" pp_node server Address.pp key)
            in
            let state = { state with values = Map.add key data state.values } in
            Lwt_result.return (state, Message.Done)
          else
            Lwt_result.return (state, Message.Not_found)
        in
        Transport.state server f
    and learn server = function
      | Message.Invalidate { address; actual } ->
        let f state =
          (* FIXME: check it's somewhat valid *)
          let current = finger server state address in
          let+ () =
            Log.debug (fun m ->
                m "%a: fix finger for %a with %a (was %a)" pp_node server
                  Address.pp address Message.pp_peer actual
                  (Opt.pp Message.pp_peer) current)
          in
          ({ state with finger = Finger.add state.finger actual }, ())
        in
        Transport.state server f
    in
    Transport.serve transport ~address ~init ~respond ~learn

  let make address ?started endpoints =
    let transport = Transport.make () in
    make_details address transport ?started endpoints

  let endpoint = Transport.endpoint

  let get server addr =
    let get state =
      successor server state addr >>= function
      | None -> (
        match Map.find_opt addr state.values with
        | Some value -> Lwt_result.return (state, value)
        | None ->
          Lwt_result.fail
            (Format.asprintf "key %a not found locally" Address.pp addr) )
      | Some ((peer, _), healed_state) -> (
        let state = Option.value ~default:state healed_state in
        let* client = Transport.connect server peer.endpoint in
        Transport.send client (Message.Get addr) >>= function
        | Message.Value v -> Lwt_result.return (state, v)
        | Message.Not_found ->
          Lwt_result.fail
            (Format.asprintf "key %a not found remotely on %a" Address.pp addr
               Message.pp_peer peer)
        | _ -> Lwt_result.fail "unexpected answer" )
    in
    Transport.state server get

  let set server key data =
    let set state =
      let* () =
        Log.debug (fun m ->
            m "%a: set %a = %S" pp_node server Address.pp key
              (Bytes.unsafe_to_string ~no_mutation_while_string_reachable:data))
      in
      successor server state key >>= function
      | None ->
        Lwt_result.return
          ({ state with values = Map.add key data state.values }, ())
      | Some ((peer, _), healed_state) -> (
        let state = Option.value ~default:state healed_state in
        let* client = Transport.connect server peer.endpoint in
        Transport.send client (Message.Set (key, data)) >>= function
        | Message.Done -> Lwt_result.return (state, ())
        | Message.Not_found ->
          Lwt_result.fail
            (Format.asprintf "key %a not set remotely on %a" Address.pp key
               Message.pp_peer peer)
        | _ -> Lwt_result.fail "unexpected answer" )
    in
    Transport.state server set

  let wait = Transport.wait

  let stop = Transport.stop
end

module Make (A : Address.S) (W : Transport.Wire) :
  Dht.S with module Address = A =
  MakeDetails (A) (W) (Transport.Make (W) (Message (A) (W)))
