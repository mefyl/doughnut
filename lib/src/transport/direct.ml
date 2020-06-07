open Utils

open Let.Syntax2 (Lwt_result)

module Make () = struct
  type t = { mutable count : int }

  type message =
    | Query of Sexp.t
    | Info of Sexp.t
    | Response of Sexp.t

  type client = {
    send : message option -> unit;
    receive : message Lwt_stream.t;
  }

  type server = {
    connections : client Lwt_stream.t;
    add_connection : client option -> unit;
    id : int;
    wait : unit Lwt.t;
    stop : unit Lwt.u;
  }

  module Endpoint = struct
    type t = server

    let pp fmt { id; _ } = Format.pp_print_int fmt id

    module Map = Stdlib.Map.Make (Int)

    let map : server Map.t ref = ref Map.empty

    let to_string { id; _ } = Int.to_string id

    let to_sexp ep = Sexp.Atom (to_string ep)

    let of_string s =
      match Caml.int_of_string_opt s with
      | Some id -> (
        match Map.find_opt id !map with
        | Some v -> Result.Ok v
        | None -> Result.Error ("no such endpoint: " ^ s) )
      | None -> Result.Error ("invalid endpoint: " ^ s)

    let of_sexp = function
      | Sexp.Atom s -> of_string s
      | sexp ->
        Result.Error (Format.asprintf "invalid endpoint: %a" Sexp.pp sexp)

    let compare { id = l; _ } { id = r; _ } = l - r
  end

  let make () = { count = 0 }

  let connect _ { add_connection; _ } =
    let client_receive, server_send = Lwt_stream.create ()
    and server_receive, client_send = Lwt_stream.create () in
    let () =
      add_connection (Some { send = server_send; receive = server_receive })
    in
    Lwt_result.return { send = client_send; receive = client_receive }

  let bind wire =
    let id = wire.count - 1
    and connections, add_connection = Lwt_stream.create ()
    and wait, stop = Lwt.wait () in
    wire.count <- wire.count + 1;
    Lwt_result.return { connections; add_connection; id; wait; stop }

  let serve server f =
    let f client =
      let open Let.Syntax (Lwt) in
      f client >>= function
      | Result.Ok () -> Lwt.return ()
      | Result.Error msg ->
        let* () = Log.warn_lwt (fun m -> m "client fatal error: %s" msg) in
        Lwt.return ()
    in
    let rec accept () =
      let open Let.Syntax (Lwt) in
      try%lwt
        let* client = Lwt_stream.next server.connections in
        Lwt.async (fun () -> f client);
        (accept [@tailcall]) ()
      with Lwt_stream.Empty -> Lwt.return @@ Lwt.wakeup server.stop ()
    in
    Lwt.async accept

  let endpoint server = server

  let send { send; _ } message = Lwt_result.return @@ send (Some message)

  let receive { receive; _ } = Lwt_stream.next receive |> lwt_ok

  let wait { wait; _ } = wait |> lwt_ok

  let stop { add_connection; _ } = add_connection None
end
