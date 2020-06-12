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
    respond :
      'state server ->
      MessageType.query Message.t ->
      (MessageType.response Message.t, string) Lwt_result.t;
    learn :
      'state server -> MessageType.info Message.t -> (unit, string) Lwt_result.t;
  }

  and 'state client = {
    client : Wire.client;
    client_address : Message.Address.t;
    client_server : 'state server;
    mutable query_id : int;
    mutable queries :
      (Int.t, (Sexp.t, string) Result.t Lwt.u, Int.comparator_witness) Map.t;
  }

  let address { address; _ } = address

  let endpoint server = Wire.endpoint server.server

  let pp_server fmt server =
    Fmt.pf fmt "node (%a)" Message.Address.pp (address server)

  let pp_client fmt { client_address; _ } =
    let f =
      let open Fmt in
      const string "peer " ++ const Address.pp client_address
    in
    f fmt ()

  type handshake_error =
    | Already_connected
    | Error of string

  let handshake server client =
    let convert_error v = Lwt_result.map_err (fun m -> Error m) v in
    let* () =
      Wire.send client
        (Info
           (Sexp.List
              [
                Sexp.Atom "doughnut";
                Sexp.Atom Message.name;
                Sexp.Atom (Semver.to_string Message.version);
                (* FIXME: check address format compatibility *)
                Message.Address.sexp_of server.address;
              ]))
      |> convert_error
    in
    let ok () =
      Wire.send client (Info (Sexp.List [ Sexp.Atom "ok" ])) |> convert_error
    and ko fmt =
      let ko reason =
        let* () =
          Wire.send client
            (Info (Sexp.List [ Sexp.Atom "ko"; Sexp.Atom reason ]))
          |> convert_error
        in
        Lwt_result.fail @@ Error reason
      in
      Format.ksprintf ko fmt
    and already_connected () =
      let* () =
        Wire.send client (Info (Sexp.List [ Sexp.Atom "duplicate" ]))
        |> convert_error
      in
      Lwt_result.fail Already_connected
    in
    let* peer_address, _peer_version =
      Wire.receive client |> convert_error >>= function
      | Info
          (Sexp.List
            [ Sexp.Atom "doughnut"; Sexp.Atom name; Sexp.Atom version; address ])
        ->
        let* () =
          if String.compare name Message.name <> 0 then
            ko "DHT type mismatch: %s <> %s" name Message.name
          else
            Lwt_result.return ()
        in
        let+ version =
          match Semver.of_string version with
          | None -> ko "invalid version: %s" version
          | Some v -> Lwt_result.return v
        and+ address =
          convert_error @@ Lwt.return @@ Message.Address.of_sexp address
        in
        (address, version)
      | Info m
      | Query m
      | Response m ->
        convert_error @@ fail "invalid handshake: %a" Sexp.pp m
    in
    let response () =
      Wire.receive client |> convert_error >>= function
      | Info (Sexp.List (Sexp.Atom "ok" :: _)) ->
        Log.info (fun m ->
            m "%a: connected to %a" pp_server server Message.Address.pp
              peer_address)
      | Info (Sexp.List [ Sexp.Atom "ko"; Sexp.Atom reason ]) ->
        let* () =
          Log.debug (fun m ->
              m "%a: peer %a rejected connection: %s" pp_server server
                Message.Address.pp peer_address reason)
        in
        Lwt_result.fail @@ Error reason
      | Info (Sexp.List [ Sexp.Atom "duplicate" ]) ->
        let* () =
          Log.debug (fun m ->
              m "%a: peer %a deems we are already connected" pp_server server
                Message.Address.pp peer_address)
        in
        Lwt_result.fail Already_connected
      | Info m
      | Query m
      | Response m ->
        fail "invalid handshake response: %a" Sexp.pp m |> convert_error
    in
    let master = Address.O.(server.address < peer_address) in
    let* () =
      if Map.mem server.peers peer_address then
        already_connected ()
      else if master then
        let* () = response () in
        ok ()
      else
        let* () = ok () in
        response ()
    in
    let client =
      {
        client;
        client_address = peer_address;
        client_server = server;
        query_id = 0;
        queries = Map.empty (module Int);
      }
    in
    let () =
      let peers =
        match Map.add server.peers ~key:peer_address ~data:client with
        | `Ok map -> map
        | `Duplicate -> failwith "duplicate peer during handshake"
      in
      server.peers <- peers
    in
    Lwt_result.return client

  let make () = ()

  let serve_client server ({ client_address = address; _ } as client) =
    let respond server sexp =
      let* query = Message.query_of_sexp sexp |> Lwt.return in
      let+ response = server.respond server query in
      Message.sexp_of_message response
    and learn server sexp =
      let* info = Message.info_of_sexp sexp |> Lwt.return in
      server.learn server info
    in
    let rec loop () =
      Wire.receive client.client >>= function
      | Query (Sexp.List [ Sexp.Atom id; query ]) ->
        let* () =
          Log.debug (fun m ->
              m "%a: receive query %s from %a: %a" pp_server server id
                Address.pp address Sexp.pp query)
        in
        let* response = respond server query in
        let* () =
          Log.debug (fun m ->
              m "%a: send response %s to %a: %a" pp_server server id Address.pp
                address Sexp.pp response)
        in
        let response = Sexp.List [ Sexp.Atom id; response ] in
        let* () = Wire.send client.client (Wire.Response response) in
        (loop [@tailcall]) ()
      | Response (Sexp.List [ Sexp.Atom id; response ]) -> (
        let* () =
          Log.debug (fun m ->
              m "%a: receive response %s from %a: %a" pp_server server id
                Address.pp address Sexp.pp response)
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
          let* () =
            Log.warn (fun m -> m "%a: invalid response: %s" pp_server server e)
          in
          (loop [@tailcall]) () )
      | Info info ->
        let* () = learn server info in
        (loop [@tailcall]) ()
      | Query q -> fail "invalid query: %a" Sexp.pp q
      | Response q -> fail "invalid response: %a" Sexp.pp q
    in
    loop ()

  let serve () ~address ~init ~respond ~learn =
    let wire = Wire.make () in
    let* wire_server = Wire.bind wire in
    let* server =
      let tmp =
        {
          address;
          peers = Map.empty (module Address);
          state = State.make ();
          server = wire_server;
          wire;
          respond = (fun _ _ -> failwith "initialization server");
          learn = (fun _ _ -> failwith "initialization server");
        }
      in
      let+ state = init tmp in
      let res =
        {
          address;
          peers = Map.empty (module Address);
          server = wire_server;
          state = State.make state;
          wire;
          respond;
          learn;
        }
      in
      let peers =
        let f peer = { peer with client_server = res } in
        Map.map tmp.peers ~f
      in
      { res with peers }
    in
    let serve_client server client =
      let* client =
        let convert_error = function
          | Error m -> m
          | Already_connected -> "already connected"
        in
        handshake server client |> Lwt_result.map_err convert_error
      in
      serve_client server client
    in
    let () = Wire.serve wire_server (serve_client server) in
    Lwt_result.return server

  let send client query =
    let id = Int.to_string client.query_id in
    let query = Sexp.List [ Sexp.Atom id; Message.sexp_of_message query ] in
    let* () =
      Log.debug (fun m ->
          m "%a: send query %s to %a: %a" pp_server client.client_server id
            pp_client client Sexp.pp query)
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
          m "%a: response from %a: %a" pp_server client.client_server
            Message.Address.pp client.client_address Sexp.pp response)
    in
    Lwt.return @@ Message.response_of_sexp response

  let inform client info =
    let info = Message.sexp_of_message info in
    let* () =
      Log.info (fun m ->
          m "%a: inform %a: %a\n%!" pp_server client.client_server pp_client
            client Sexp.pp info)
    in
    Wire.send client.client (Wire.Info info)

  let state server f = State.run server.state f

  let connect server endpoint =
    let* client = Wire.connect server.wire endpoint in
    let open Let.Syntax (Lwt) in
    handshake server client >>= function
    | Result.Ok client ->
      let () =
        let f () =
          serve_client server client >>= function
          | Result.Ok () -> Lwt.return ()
          | Result.Error msg ->
            Log.warn_lwt (fun m ->
                m "%a: %a error: %s" pp_server server pp_client client msg)
        in
        Lwt.async f
      in
      Lwt_result.return client
    | Result.Error (Error m) -> Lwt_result.fail m
    | Result.Error Already_connected -> failwith "youplaboum"

  let wait server = Wire.wait server.server

  let stop server = Wire.stop server.server
end

module Direct () = Direct.Make ()

module Tcp = Tcp
