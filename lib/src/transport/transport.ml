open Utils
include Transport_intf

open Let.Syntax2 (Lwt_result)

module Make (W : Wire) (M : Message) = struct
  module Address = M.Address
  module Endpoint = W.Endpoint
  module Message = M
  module Wire = W

  type t = unit

  type 'state server = {
    address : Message.Address.t;
    mutable peers :
      (Address.t, 'state client, Address.comparator_witness) Map.t;
    server : Wire.server;
    state : 'state State.t;
    wire : Wire.t;
  }

  and 'state client = {
    client : Wire.client;
    client_address : Message.Address.t;
    transport : 'state server;
    mutable query_id : int;
    mutable queries :
      (Int.t, (Sexp.t, string) Result.t Lwt.u, Int.comparator_witness) Map.t;
  }

  let handshake state client =
    let* () =
      Wire.send client
        (Info
           (Sexp.List
              [
                Sexp.Atom "doughnut";
                Sexp.Atom Message.name;
                Sexp.Atom (Semver.to_string Message.version);
                (* FIXME: check address format compatibility *)
                Message.Address.sexp_of state.address;
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
      | Info (Sexp.List (Sexp.Atom "ok" :: _)) ->
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
    let master = Address.O.(state.address < peer_address) in
    let* () =
      let ok () = Wire.send client (Info (Sexp.List [ Sexp.Atom "ok" ]))
      and ko reason =
        Wire.send client (Info (Sexp.List [ Sexp.Atom "ko"; Sexp.Atom reason ]))
      in
      if Map.mem state.peers peer_address then
        ko "already connected"
      else if master then
        let* () = response () in
        ok ()
      else
        let* () = ok () in
        response ()
    in
    Lwt_result.return (peer_address, peer_version)

  let make () = ()

  let serve () ~address ~init ~respond ~learn =
    let wire = Wire.make () in
    let* server = Wire.bind wire in
    let* transport =
      let tmp =
        {
          address;
          peers = Map.empty (module Address);
          state = State.make ();
          server;
          wire;
        }
      in
      let+ state = init tmp in
      let res =
        {
          address;
          peers = Map.empty (module Address);
          server;
          state = State.make state;
          wire;
        }
      in
      let peers =
        let f peer = { peer with transport = res } in
        Map.map tmp.peers ~f
      in
      { res with peers }
    in
    let serve_client client =
      let* client_address, _ = handshake transport client in
      let client =
        {
          client;
          client_address;
          transport;
          query_id = 0;
          queries = Map.empty (module Int);
        }
      in
      let respond server sexp =
        let* query = Message.query_of_sexp sexp |> Lwt.return in
        let+ response = respond server query in
        Message.sexp_of_message response
      and learn server sexp =
        let* info = Message.info_of_sexp sexp |> Lwt.return in
        learn server info
      in
      let rec loop () =
        Wire.receive client.client >>= function
        | Query (Sexp.List [ Sexp.Atom id; query ]) ->
          let* () =
            Log.debug (fun m ->
                m "receive query %s from %a: %a" id Address.pp client_address
                  Sexp.pp query)
          in
          let* response = respond transport query in
          let* () =
            Log.debug (fun m ->
                m "send response %s to %a: %a" id Address.pp client_address
                  Sexp.pp response)
          in
          let response = Sexp.List [ Sexp.Atom id; response ] in
          let* () = Wire.send client.client (Wire.Response response) in
          (loop [@tailcall]) ()
        | Response (Sexp.List [ Sexp.Atom id; response ]) -> (
          let* () =
            Log.debug (fun m ->
                m "receive response %s from %a: %a" id Address.pp client_address
                  Sexp.pp response)
          in
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
          let* () = learn transport info in
          (loop [@tailcall]) ()
        | Query q -> fail "invalid query: %a" Sexp.pp q
        | Response q -> fail "invalid response: %a" Sexp.pp q
      in
      loop ()
    in
    let () = Wire.serve server serve_client in
    Lwt_result.return transport

  let send client query =
    let id = Int.to_string client.query_id in
    let query = Sexp.List [ Sexp.Atom id; Message.sexp_of_message query ] in
    let* () =
      Log.debug (fun m ->
          m "send query %s to %a: %a" id Message.Address.pp
            client.client_address Sexp.pp query)
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
    let* () =
      Log.debug (fun m ->
          m "response from %a: %a" Message.Address.pp client.client_address
            Sexp.pp response)
    in
    Lwt.return @@ Message.response_of_sexp response

  let inform client info =
    let info = Message.sexp_of_message info in
    let* () =
      Log.info (fun m ->
          m "inform %a: %a\n%!" Message.Address.pp client.client_address Sexp.pp
            info)
    in
    Wire.send client.client (Wire.Info info)

  let state server f = State.run server.state f

  let connect transport endpoint =
    let* client = Wire.connect transport.wire endpoint in
    let+ client_address, _ = handshake transport client in
    {
      client_address;
      transport;
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
