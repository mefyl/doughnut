open Utils

open Let.Syntax2 (Lwt_result)

module Make () = struct
  type client = {
    id : int;
    rpc : (Sexp.t, Sexp.t) Rpc.t;
    info : Sexp.t Lwt_stream.t * (Sexp.t option -> unit);
  }

  type 'state server = {
    client : client;
    state : 'state State.t;
  }

  module Endpoint = struct
    type t = client

    let pp fmt { id; _ } = Format.pp_print_int fmt id

    module Map = Stdlib.Map.Make (Int)

    let map : client Map.t ref = ref Map.empty

    let to_string ({ id; _ } as ep) =
      map := Map.add id ep !map;
      Int.to_string id

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

  let count = ref 0

  type t = unit

  type id = unit

  let make () = ()

  let connect _ ep = Lwt_result.return ep

  type message =
    | Query of Sexp.t
    | Info of Sexp.t

  let serve ~init ~respond ~learn () =
    let server =
      let id = !count
      and rpc = Rpc.make ()
      and info = Lwt_stream.create () in
      { id; rpc; info }
    in
    let () = count := !count + 1 in
    let* state = init server in
    let state = State.make state in
    let res = { client = server; state } in
    let serve () =
      let read_query () =
        let open Let.Syntax (Lwt) in
        let+ query = Rpc.receive server.rpc in
        Result.return @@ Query query
      and read_info () =
        let open Let.Syntax (Lwt) in
        let+ info = Lwt_stream.next (fst server.info) in
        Result.return @@ Info info
      in
      let rec loop query info =
        Lwt.choose [ query; info ] >>= function
        | Query query ->
          let* () = Log.debug (fun m -> m "receive query %a" Sexp.pp query) in
          let* response =
            let action state = respond state query in
            State.run state action
          in
          let* () = Log.debug (fun m -> m "respond %a" Sexp.pp response) in
          let* () = Rpc.respond server.rpc response |> lwt_ok in
          (loop [@tailcall]) (read_query ()) info
        | Info info ->
          let* () =
            Log.debug (fun m -> m "receive information %a" Sexp.pp info)
          in
          let* () =
            let action state =
              let+ state = learn state info in
              (state, ())
            in
            State.run state action
          in
          (loop [@tailcall]) query (read_info ())
      in
      let open Lwt.Infix in
      loop (read_query ()) (read_info ()) >>= function
      | Result.Ok () -> Lwt.return ()
      | Result.Error e ->
        Logs_lwt.warn ~src:Log.src (fun m ->
            m "direct transport fatal error: %s" e)
    in
    let () = Lwt.async serve in
    Lwt_result.return res

  let endpoint _ s = s.client

  let send _ { rpc; _ } query =
    Logs.debug (fun m -> m "send %a" Sexp.pp query);
    Rpc.send rpc query |> Lwt.map ~f:Result.return

  let inform _ { info = _, send; _ } info =
    Lwt_result.return @@ send (Some info)

  let state { state; _ } f = State.run state f

  let wait { state; _ } =
    let+ _final_state = State.wait state in
    ()

  let stop { state; _ } = State.stop state
end
