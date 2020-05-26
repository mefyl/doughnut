open Utils

module Make () = struct
  type server =
    int
    * (Sexp.t, Sexp.t) Rpc.t
    * (Sexp.t Lwt_stream.t * (Sexp.t option -> unit))

  type client = server

  type endpoint = server

  module Map = Stdlib.Map.Make (Int)

  let count = ref 0

  let map : server Map.t ref = ref Map.empty

  let pp_endpoint fmt (id, _, _) = Format.pp_print_int fmt id

  let sexp_of_endpoint ((id, _, _) as ep) =
    map := Map.add id ep !map;
    Sexp.Atom (Int.to_string id)

  let endpoint_of_sexp = function
    | Sexp.Atom s -> (
      match Caml.int_of_string_opt s with
      | Some id -> (
        match Map.find_opt id !map with
        | Some v -> Result.Ok v
        | None -> Result.Error ("no such endpoint: " ^ s) )
      | None -> Result.Error ("invalid endpoint: " ^ s) )
    | sexp -> Result.Error (Format.asprintf "invalid endpoint: %a" Sexp.pp sexp)

  type t = unit

  type id = unit

  let make () = ()

  let connect _ ep = ep

  let listen _ () =
    count := !count + 1;
    (!count - 1, Rpc.make (), Lwt_stream.create ())

  let endpoint _ s = s

  let send _ (_, rpc, _) query =
    Logs.debug (fun m -> m "send %a" Sexp.pp query);
    Rpc.send rpc query

  let receive _ (_, rpc, _) =
    let open Let.Syntax (Lwt) in
    let+ query = Rpc.receive rpc in
    Logs.debug (fun m -> m "receive %a" Sexp.pp query);
    ((), query)

  let respond _ (_, rpc, _) () resp =
    Logs.debug (fun m -> m "respond %a" Sexp.pp resp);
    Rpc.respond rpc resp

  let inform _ (_, _, (_, send)) info = send (Some info)

  let learn _ (_, _, (stream, _)) =
    let open Let.Syntax (Lwt) in
    let+ info = Lwt_stream.next stream in
    Logs.debug (fun m -> m "receive information %a" Sexp.pp info);
    info
end
