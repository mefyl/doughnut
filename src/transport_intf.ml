open Core

module MessagesType = struct
  type query = Query

  type response = Response

  type info = Info
end

module type Messages = sig
  type 'a message

  open MessagesType

  val sexp_of_message : 'a message -> Sexp.t

  val query_of_sexp : Sexp.t -> (query message, string) Result.t

  val response_of_sexp : Sexp.t -> (response message, string) Result.t

  val info_of_sexp : Sexp.t -> (info message, string) Result.t
end

module type Wire = sig
  type client

  type server

  type endpoint

  val pp_endpoint : Format.formatter -> endpoint -> unit

  val sexp_of_endpoint : endpoint -> Sexp.t

  val endpoint_of_sexp : Sexp.t -> (endpoint, string) Result.t

  type t

  val make : unit -> t

  val connect : t -> endpoint -> client

  val listen : t -> unit -> server

  val endpoint : t -> server -> endpoint

  type id

  val send : t -> client -> Sexp.t -> Sexp.t Lwt.t

  val receive : t -> server -> (id * Sexp.t) Lwt.t

  val respond : t -> server -> id -> Sexp.t -> unit Lwt.t

  val inform : t -> client -> Sexp.t -> unit

  val learn : t -> server -> Sexp.t Lwt.t
end

module type Transport = sig
  module Messages : Messages

  module Wire : Wire

  type server = Wire.server

  type client = Wire.client

  type endpoint = Wire.endpoint

  type id = Wire.id

  type t

  val wire : t -> Wire.t

  val make : unit -> t

  val connect : t -> endpoint -> client

  val listen : t -> unit -> server

  val endpoint : t -> server -> endpoint

  val send :
    t ->
    client ->
    MessagesType.query Messages.message ->
    (MessagesType.response Messages.message, string) Lwt_result.t

  val receive : t -> server -> (id * MessagesType.query Messages.message) Lwt.t

  val respond :
    t -> server -> id -> MessagesType.response Messages.message -> unit Lwt.t

  val inform : t -> client -> MessagesType.info Messages.message -> unit

  val learn : t -> server -> MessagesType.info Messages.message Lwt.t
end
