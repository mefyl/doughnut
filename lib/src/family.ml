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

    let sexp_of_message : type k. k t -> Sexp.t = function
      | Join e -> Sexp.List [ Sexp.Atom "join"; Peer.to_sexp e ]
      | List -> Sexp.Atom "list"
      | Listed peers ->
        Sexp.List
          [ Sexp.Atom "list"; Sexp.List (List.map ~f:Peer.to_sexp peers) ]

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
      | i -> Result.fail (Fmt.str "invalid info: %a" Sexp.pp i)
  end

  module Transport = Transport.Make (Wire) (Message)

  type state = { peers : (Peer.t, Peer.comparator_witness) Set.t }

  type node = {
    address : Address.t;
    server : state Transport.server;
    transport : Transport.t;
  }

  open Let.Syntax2 (Lwt_result)

  let make address endpoints =
    let* () = Log.debug (fun m -> m "family(%a): make" Address.pp address) in
    let transport = Transport.make () in
    let init endpoint =
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
        match result with
        | Result.Error e ->
          let%lwt () =
            Log.warn_lwt (fun m ->
                m "unable to connect to peer %a: %s" Transport.Wire.Endpoint.pp
                  peer e)
          in
          Lwt.return []
        | Result.Ok peers -> Lwt.return peers
      in
      let%lwt peers = Lwt_list.map_p f endpoints in
      let peers = List.concat peers in
      let* () =
        Log.info (fun m -> m "connected to %i peers" (List.length peers))
      in
      Lwt_result.return { peers = Set.of_list (module Peer) peers }
    and respond state = function
      | Message.Join peer ->
        Lwt_result.return
          ( { peers = Set.add state.peers peer },
            Message.Listed (Set.to_list state.peers) )
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
