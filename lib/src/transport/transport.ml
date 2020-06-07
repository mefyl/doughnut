open Utils
include Transport_intf

open Let.Syntax2 (Lwt_result)

module Make (W : Wire) (M : Message) = struct
  module Address = M.Address
  module Endpoint = W.Endpoint
  module Message = M
  module Wire = W

  type t = {
    address : Message.Address.t;
    wire : Wire.t;
    mutable peers : (Address.t, client, Address.comparator_witness) Map.t;
  }

  and client = {
    client_transport : t;
    client : Wire.client;
    mutable query_id : int;
    mutable queries :
      (Int.t, (Sexp.t, string) Result.t Lwt.u, Int.comparator_witness) Map.t;
  }

  and 'state server = {
    server_transport : t;
    server : Wire.server;
    state : 'state State.t;
  }

  type endpoint = Wire.Endpoint.t

  let wire { wire; _ } = wire

  let make address =
    { address; wire = Wire.make (); peers = Map.empty (module Address) }

  let handshake t client =
    let* () =
      Wire.send client
        (Info
           (Sexp.List
              [
                Sexp.Atom "doughnut";
                Sexp.Atom Message.name;
                Sexp.Atom (Semver.to_string Message.version);
                (* FIXME: check address format compatibility *)
                Message.Address.sexp_of t.address;
              ]))
    in
    let* peer_address, peer_version =
      Wire.receive client >>= function
      | Info
          (Sexp.List
            [ Sexp.Atom "doughnut"; Sexp.Atom name; Sexp.Atom version; address ])
        ->
        let open Let.Syntax2 (Result) in
        Lwt.return
        @@ let+ () =
             if String.compare name Message.name <> 0 then
               Result.failf "DHT type mismatch: %s <> %s" name Message.name
             else
               Result.return ()
           and+ version =
             match Semver.of_string version with
             | None -> Result.failf "invalid version: %s" version
             | Some v -> Result.return v
           and+ address = Message.Address.of_sexp address in
           (address, version)
      | Info m
      | Query m
      | Response m ->
        fail "invalid handshake: %a" Sexp.pp m
    in
    let response () =
      Wire.receive client >>= function
      | Info (Sexp.List [ Sexp.Atom "ok"; _ ]) ->
        Log.info (fun m -> m "connected to %a" Message.Address.pp peer_address)
      | Info (Sexp.List [ Sexp.Atom "ko"; Sexp.Atom reason ]) ->
        Log.debug (fun m ->
            m "peer %a rejected connection: %s" Message.Address.pp peer_address
              reason)
      | Info m
      | Query m
      | Response m ->
        fail "invalid handshake response: %a" Sexp.pp m
    in
    let master = Address.O.(t.address < peer_address) in
    let* () =
      let ok () = Wire.send client (Info (Sexp.List [ Sexp.Atom "ok" ]))
      and ko reason =
        Wire.send client (Info (Sexp.List [ Sexp.Atom "ko"; Sexp.Atom reason ]))
      in
      if Map.mem t.peers peer_address then
        ko "already connected"
      else if master then
        let* () = response () in
        ok ()
      else
        let* () = ok () in
        response ()
    in
    Lwt_result.return (peer_address, peer_version)

  let serve ~init ~respond ~learn transport =
    let wire = Wire.make () in
    let* server = Wire.bind wire in
    let endpoint = Wire.endpoint server in
    let* state =
      let+ state = init endpoint in
      State.make state
    in
    let res = { server_transport = transport; server; state } in
    let serve_client client =
      let client =
        {
          client_transport = transport;
          client;
          query_id = 0;
          queries = Map.empty (module Int);
        }
      in
      let* _peer_address, _ = handshake transport client.client in
      let respond state sexp =
        let* query = Message.query_of_sexp sexp |> Lwt.return in
        let+ state, response = respond state query in
        (state, Message.sexp_of_message response)
      and learn state sexp =
        let* info = Message.info_of_sexp sexp |> Lwt.return in
        learn state info
      in
      let rec loop () =
        Wire.receive client.client >>= function
        | Query (Sexp.List [ Sexp.Atom id; query ]) ->
          let* response =
            let action state = respond state query in
            State.run state action
          in
          let response = Sexp.List [ Sexp.Atom id; response ] in
          let* () = Wire.send client.client (Wire.Response response) in
          (loop [@tailcall]) ()
        | Response (Sexp.List [ Sexp.Atom id; response ]) -> (
          match
            let open Let.Syntax2 (Result) in
            let* id =
              try Result.return @@ Int.of_string id
              with Failure _ -> Result.failf "invalid id: %s" id
            in
            match Map.find client.queries id with
            | Some resolver ->
              client.queries <- Map.remove client.queries id;
              Result.return @@ Lwt.wakeup resolver (Result.return response)
            | None -> Result.failf "unknown id: %i" id
          with
          | Result.Ok () -> (loop [@tailcall]) ()
          | Result.Error e ->
            let* () = Log.warn (fun m -> m "invalid response: %s" e) in
            (loop [@tailcall]) () )
        | Info info ->
          let* () =
            let action state =
              let+ state = learn state info in
              (state, ())
            in
            State.run state action
          in
          (loop [@tailcall]) ()
        | Query q -> fail "invalid query: %a" Sexp.pp q
        | Response q -> fail "invalid response: %a" Sexp.pp q
      in
      loop ()
    in
    let () = Wire.serve server serve_client in
    Lwt_result.return res

  let send client query =
    let query =
      Sexp.List
        [
          Sexp.Atom (Int.to_string client.query_id);
          Message.sexp_of_message query;
        ]
    in
    let id =
      client.query_id <- client.query_id + 1;
      client.query_id - 1
    and wait, resolve = Lwt.wait () in
    let () =
      client.queries <-
        ( match Map.add client.queries ~key:id ~data:resolve with
        | `Ok map -> map
        | `Duplicate -> failwith "duplicate query id" )
    in
    let* () = Wire.send client.client @@ Wire.Query query in
    let* response = wait in
    Lwt.return @@ Message.response_of_sexp response

  let inform client info =
    Wire.send client.client (Wire.Info (Message.sexp_of_message info))

  let state server f = State.run server.state f

  let connect transport endpoint =
    let+ client = Wire.connect (wire transport) endpoint in
    {
      client_transport = transport;
      client;
      query_id = 0;
      queries = Map.empty (module Int);
    }

  let endpoint server = Wire.endpoint server.server

  let wait server = Wire.wait server.server

  let stop server = Wire.stop server.server
end

module Direct () = Direct.Make ()

module Tcp = Tcp
