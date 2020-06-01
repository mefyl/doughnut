open Utils
include Transport_intf

open Let.Syntax2 (Lwt_result)

module Make (W : Wire) (M : Message) = struct
  module Message = M
  module Wire = W
  include Wire

  type endpoint = Wire.Endpoint.t

  let wire x = x

  let send t client query =
    let* response = Wire.send (wire t) client (Message.sexp_of_message query) in
    Lwt.return @@ Message.response_of_sexp response

  let inform t client info =
    Wire.inform (wire t) client (Message.sexp_of_message info)

  let serve ~init ~respond ~learn t =
    let respond state sexp =
      let* query = Message.query_of_sexp sexp |> Lwt.return in
      let+ state, response = respond state query in
      (state, Message.sexp_of_message response)
    and learn state sexp =
      let* info = Message.info_of_sexp sexp |> Lwt.return in
      learn state info
    in
    Wire.serve ~init ~respond ~learn (wire t)

  let state server f = Wire.state server f
end

module Direct () = Direct.Make ()

module Tcp = Tcp
