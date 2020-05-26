open Base

include Transport_intf

module Format = Caml.Format

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
    (!count - 1, Lwt_utils.RPC.make (), Lwt_stream.create ())

  let endpoint _ s = s

  let send _ (_, rpc, _) query =
    Logs.debug (fun m -> m "send %a" Sexp.pp query);
    Lwt_utils.RPC.send rpc query

  let receive _ (_, rpc, _) =
    let open Lwt_utils.O in
    let+ query = Lwt_utils.RPC.receive rpc in
    Logs.debug (fun m -> m "receive %a" Sexp.pp query);
    ((), query)

  let respond _ (_, rpc, _) () resp =
    Logs.debug (fun m -> m "respond %a" Sexp.pp resp);
    Lwt_utils.RPC.respond rpc resp

  let inform _ (_, _, (_, send)) info = send (Some info)

  let learn _ (_, _, (stream, _)) =
    let open Lwt_utils.O in
    let+ info = Lwt_stream.next stream in
    Logs.debug (fun m -> m "receive information %a" Sexp.pp info);
    info
end

module Make (W : Wire) (M : Messages) = struct
  module Messages = M
  module Wire = W
  include Wire

  let wire x = x

  let send t client query =
    let open Lwt_utils.O in
    let+ response =
      Wire.send (wire t) client (Messages.sexp_of_message query)
    in
    Messages.response_of_sexp response

  let rec receive t server =
    let open Lwt_utils.O in
    let* id, query = Wire.receive (wire t) server in
    match Messages.query_of_sexp query with
    | Result.Ok query -> Lwt.return (id, query)
    | Result.Error s ->
        let* () = Logs_lwt.warn (fun m -> m "error receiving query: %s" s) in
        receive (wire t) server

  let respond t server id response =
    Wire.respond (wire t) server id (Messages.sexp_of_message response)

  let inform t client info =
    Wire.inform (wire t) client (Messages.sexp_of_message info)

  let rec learn t server =
    let open Lwt_utils.O in
    let* info = Wire.learn (wire t) server in
    match Messages.info_of_sexp info with
    | Result.Ok info -> Lwt.return info
    | Result.Error s ->
        let* () = Logs_lwt.warn (fun m -> m "error receiving info: %s" s) in
        learn (wire t) server
end
