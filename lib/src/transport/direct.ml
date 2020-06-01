open Utils

open Let.Syntax2 (Lwt_result)

module Make () = struct
  type client = {
    id : int;
    rpc : (Sexp.t, Sexp.t) Rpc.t;
    info : Sexp.t Lwt_stream.t * (Sexp.t option -> unit);
  }

  type 'state state_action =
    | Action :
        ('state -> ('state * 'result, string) Lwt_result.t)
        * ('result, string) Result.t Lwt.u
        -> 'state state_action

  type 'state action =
    | Query of Sexp.t
    | Info of Sexp.t
    | State of 'state state_action

  type 'state server = {
    client : client;
    actions : 'state state_action Lwt_stream.t;
    push_action : 'state state_action option -> unit;
    mutable state : 'state;
  }

  module Endpoint = struct
    type t = client

    let pp fmt { id; _ } = Format.pp_print_int fmt id

    module Map = Stdlib.Map.Make (Int)

    let map : client Map.t ref = ref Map.empty

    let to_sexp ({ id; _ } as ep) =
      map := Map.add id ep !map;
      Sexp.Atom (Int.to_string id)

    let of_sexp = function
      | Sexp.Atom s -> (
        match Caml.int_of_string_opt s with
        | Some id -> (
          match Map.find_opt id !map with
          | Some v -> Result.Ok v
          | None -> Result.Error ("no such endpoint: " ^ s) )
        | None -> Result.Error ("invalid endpoint: " ^ s) )
      | sexp ->
        Result.Error (Format.asprintf "invalid endpoint: %a" Sexp.pp sexp)

    let compare { id = l; _ } { id = r; _ } = l - r
  end

  let count = ref 0

  type t = unit

  type id = unit

  let make () = ()

  let connect _ ep = Lwt_result.return ep

  let serve ~init ~respond ~learn () =
    let server =
      let id = !count
      and rpc = Rpc.make ()
      and info = Lwt_stream.create () in
      { id; rpc; info }
    in
    let () = count := !count + 1 in
    let* state = init server in
    let actions, push_action = Lwt_stream.create () in
    let res = { client = server; state; actions; push_action } in
    let serve () =
      let read_query () =
        let open Let.Syntax (Lwt) in
        let+ query = Rpc.receive server.rpc in
        Result.return @@ Query query
      and read_info () =
        let open Let.Syntax (Lwt) in
        let+ info = Lwt_stream.next (fst server.info) in
        Result.return @@ Info info
      and get_action () =
        let open Let.Syntax (Lwt) in
        let+ action = Lwt_stream.next actions in
        Result.return @@ State action
      in
      let rec loop state query info action =
        Lwt.choose [ query; info; action ] >>= function
        | Query query ->
          let* () = Log.debug (fun m -> m "receive query %a" Sexp.pp query) in
          let* state, response = respond state query in
          let* () = Log.debug (fun m -> m "respond %a" Sexp.pp response) in
          let* () = Rpc.respond server.rpc response |> lwt_ok in
          (loop [@tailcall]) state (read_query ()) info action
        | Info info ->
          let* () =
            Log.debug (fun m -> m "receive information %a" Sexp.pp info)
          in
          let* state = learn state info in
          (loop [@tailcall]) state query (read_info ()) action
        | State (Action (f, resolver)) -> (
          let* () = Log.debug (fun m -> m "apply state mutator") in
          let open Lwt.Infix in
          f state >>= function
          | Result.Ok (state, result) ->
            let () = Lwt.wakeup resolver (Result.Ok result) in
            (loop [@tailcall]) state query info (get_action ())
          | Result.Error e ->
            let () = Lwt.wakeup resolver (Result.Error e) in
            (loop [@tailcall]) state query info (get_action ()) )
      in
      let open Lwt.Infix in
      loop state (read_query ()) (read_info ()) (get_action ()) >>= function
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

  let state { push_action; _ } f =
    let wait, resolve = Lwt.wait () in
    let () = push_action (Some (Action (f, resolve))) in
    wait
end
