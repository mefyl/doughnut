open Utils
include Transport_intf

module Make (W : Wire) (M : Messages) = struct
  module Messages = M
  module Wire = W
  include Wire

  let wire x = x

  let send t client query =
    let open Let.Syntax (Lwt) in
    let+ response =
      Wire.send (wire t) client (Messages.sexp_of_message query)
    in
    Messages.response_of_sexp response

  let rec receive t server =
    let open Let.Syntax (Lwt) in
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
    let open Let.Syntax (Lwt) in
    let* info = Wire.learn (wire t) server in
    match Messages.info_of_sexp info with
    | Result.Ok info -> Lwt.return info
    | Result.Error s ->
      let* () = Logs_lwt.warn (fun m -> m "error receiving info: %s" s) in
      learn (wire t) server
end

module Direct () = Direct.Make ()
