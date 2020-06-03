open Utils

open Let.Syntax2 (Result)

module Make (A : Address.S) (W : Transport.Wire) : Allocator.S = struct
  module Address = A
  module Wire = W

  type endpoint = W.Endpoint.t

  module Peer = struct
    module T = struct
      type t = Address.t * endpoint

      let compare (laddr, lep) (raddr, rep) =
        match Address.compare laddr raddr with
        | 0 -> W.Endpoint.compare lep rep
        | res -> res

      let sexp_of_t (address, ep) =
        Sexp.List [ Address.sexp_of address; Wire.Endpoint.to_sexp ep ]

      let to_sexp = sexp_of_t

      let of_sexp = function
        | Sexp.List [ address; ep ] ->
          let open Let.Syntax2 (Result) in
          let+ address = Address.of_sexp address
          and+ ep = Wire.Endpoint.of_sexp ep in
          (address, ep)
        | _ -> Result.fail "invalid peer"

      let pp =
        let open Fmt in
        pair ~sep:(const char '@') Address.pp Wire.Endpoint.pp
    end

    include T
    include Comparator.Make (T)
  end

  module Message = struct
    open Transport.MessageType

    type _ t =
      | Join : Peer.t -> query t
      | List : query t
      | Listed : Peer.t list -> response t
      | Newcomer : Peer.t -> info t

    let sexp_of_message : type k. k t -> Sexp.t = function
      | Join e -> Sexp.List [ Sexp.Atom "join"; Peer.to_sexp e ]
      | List -> Sexp.Atom "list"
      | Listed peers ->
        Sexp.List
          [ Sexp.Atom "list"; Sexp.List (List.map ~f:Peer.to_sexp peers) ]
      | Newcomer peer -> Sexp.List [ Sexp.Atom "newcomer"; Peer.to_sexp peer ]

    let query_of_sexp = function
      | Sexp.List [ Sexp.Atom "join"; ep ] ->
        let* peer = Peer.of_sexp ep in
        Result.return (Join peer)
      | Sexp.Atom "list" -> Result.return List
      | q -> Result.fail (Fmt.str "invalid query: %a" Sexp.pp q)

    let response_of_sexp = function
      | Sexp.List [ Sexp.Atom "list"; Sexp.List peers ] ->
        let module List = List_monadic.Make2 (Result) in
        let+ peers = List.map ~f:Peer.of_sexp peers in
        Listed peers
      | r -> Result.fail (Fmt.str "invalid response: %a" Sexp.pp r)

    let info_of_sexp = function
      | Sexp.List [ Sexp.Atom "newcomer"; peer ] ->
        let+ peer = Peer.of_sexp peer in
        Newcomer peer
      | i -> Result.fail (Fmt.str "invalid info: %a" Sexp.pp i)
  end

  module Transport = Transport.Make (Wire) (Message)

  type state = {
    endpoint : Wire.Endpoint.t;
    peers : (Peer.t, Peer.comparator_witness) Set.t;
  }

  type node = {
    address : Address.t;
    server : state Transport.server;
    transport : Transport.t;
  }

  open Let.Syntax2 (Lwt_result)

  let result_warn default result fmt =
    match result with
    | Result.Ok value ->
      Format.ikfprintf (fun _fmt -> Lwt.return value) Format.str_formatter fmt
    | Result.Error e ->
      let f msg =
        let%lwt () = Log.warn_lwt (fun m -> m "%s: %s" msg e) in
        Lwt.return default
      in
      Format.kasprintf f fmt

  let report_state state =
    Log.info (fun m -> m "connected to %i peers" (Set.length state.peers))

  let make ?(started = fun _ -> Lwt_result.return ()) address endpoints =
    let* () = Log.debug (fun m -> m "family(%a): make" Address.pp address) in
    let transport = Transport.make () in
    let init endpoint =
      let* () = started endpoint in
      let* () =
        Log.debug (fun m ->
            m "family(%a): listening on %a" Address.pp address
              Transport.Wire.Endpoint.pp endpoint)
      in
      let f peer =
        let%lwt result =
          let* peer = Transport.connect transport peer in
          Transport.send transport peer (Message.Join (address, endpoint))
          >>= function
          | Listed peers -> Lwt_result.return peers
        in
        result_warn [] result "unable to connect to peer %a"
          Transport.Wire.Endpoint.pp peer
      in
      let%lwt peers = Lwt_list.map_p f endpoints in
      let peers = List.concat peers in
      let state = { endpoint; peers = Set.of_list (module Peer) peers } in
      let* () = report_state state in
      Lwt_result.return state
    and respond state = function
      | Message.Join peer ->
        let broadcast peer =
          let f ((_, endpoint) as n) =
            let%lwt res =
              let* client = Transport.connect transport endpoint in
              Transport.inform transport client (Newcomer peer)
            in
            result_warn () res "unable to notify %a of newcomer peer" Peer.pp n
          in
          Lwt_list.iter_p f (Set.to_list state.peers) |> lwt_ok
        in
        let peers = Set.to_list state.peers in
        let* state =
          if not @@ Set.mem state.peers peer then
            let* () =
              Log.debug (fun m -> m "joined by new peer %a" Peer.pp peer)
            in
            let* () = broadcast peer in
            let state = { state with peers = Set.add state.peers peer } in
            let* () = report_state state in
            Lwt_result.return state
          else
            Lwt_result.return state
        in
        Lwt_result.return
          (state, Message.Listed ((address, state.endpoint) :: peers))
      | List ->
        Lwt_result.return (state, Message.Listed (Set.to_list state.peers))
    and learn state = function
      | _ -> Lwt_result.return state
    in
    let* server = Transport.serve ~init ~respond ~learn transport in
    let res = { address; server; transport } in
    Lwt_result.return res

  let endpoint { server; transport; _ } = Transport.endpoint transport server

  let allocate _ _ = failwith "not implemented"

  let wait { server; _ } = Transport.wait server

  let stop { server; _ } = Transport.stop server
end
