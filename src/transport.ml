open Core
open Sexplib
include Transport_intf

module Direct = struct
  type server =
    int
    * (Sexp.t, Sexp.t) Lwt_utils.RPC.t
    * (Sexp.t Lwt_stream.t * (Sexp.t option -> unit))

  type client = server

  type endpoint = server

  module Map = Stdlib.Map.Make (Int)

  let count = ref 0

  let map = ref Map.empty

  let pp_endpoint fmt (id, _, _) = Format.pp_print_int fmt id

  let sexp_of_endpoint ((id, _, _) as ep) =
    map := Map.add id ep !map ;
    Sexp.Atom (string_of_int id)

  let endpoint_of_sexp = function
    | Sexp.Atom s -> (
      match int_of_string_opt s with
      | Some id -> (
        match Map.find_opt id !map with
        | Some v ->
            Result.Ok v
        | None ->
            Result.Error ("no such endpoint: " ^ s) )
      | None ->
          Result.Error ("invalid endpoint: " ^ s) )
    | sexp ->
        Result.Error (Format.asprintf "invalid endpoint: %a" Sexp.pp sexp)

  type t = unit

  type id = unit

  let make () = ()

  let connect _ ep = ep

  let listen _ () =
    count := !count + 1 ;
    (!count - 1, Lwt_utils.RPC.make (), Lwt_stream.create ())

  let endpoint _ s = s

  let send _ (_, rpc, _) query =
    Logs.debug (fun m -> m "send %a" Sexp.pp query) ;
    Lwt_utils.RPC.send rpc query

  let receive _ (_, rpc, _) =
    Lwt.bind (Lwt_utils.RPC.receive rpc) (fun query ->
        Logs.debug (fun m -> m "receive %a" Sexp.pp query) ;
        Lwt.return ((), query))

  let respond _ (_, rpc, _) () resp =
    Logs.debug (fun m -> m "respond %a" Sexp.pp resp) ;
    Lwt_utils.RPC.respond rpc resp

  let inform _ (_, _, (_, send)) info = send (Some info)
end

module Make (W : Wire) (M : Messages) = struct
  module Messages = M
  module Wire = W
  include Wire

  let wire x = x

  let send t client query =
    let open Lwt_utils.O in
    let+ response = Wire.send (wire t) client (Messages.sexp_of_query query) in
    Messages.response_of_sexp response

  let rec receive t server =
    let open Lwt_utils.O in
    let* id, query = Wire.receive (wire t) server in
    match Messages.query_of_sexp query with
    | Result.Ok query ->
        Lwt.return (id, query)
    | Result.Error s ->
        let* () = Logs_lwt.warn (fun m -> m "error receiving query: %s" s) in
        receive (wire t) server

  let respond t server id response =
    Wire.respond (wire t) server id (Messages.sexp_of_response response)

  let inform t client info =
    Wire.inform (wire t) client (Messages.sexp_of_info info)
end
