open Base

module type Wire = sig
  type t

  type client

  type server

  module Endpoint : sig
    type t

    val pp : Formatter.t -> t -> unit

    val to_string : t -> string

    val of_string : string -> (t, string) Result.t

    val to_sexp : t -> Sexp.t

    val of_sexp : Sexp.t -> (t, string) Result.t

    val compare : t -> t -> int
  end

  val make : unit -> t

  val connect : t -> Endpoint.t -> (client, string) Lwt_result.t

  val bind : t -> (server, string) Lwt_result.t

  val serve : server -> (client -> (unit, string) Lwt_result.t) -> unit

  val wait : server -> (unit, string) Lwt_result.t

  val stop : server -> unit

  val endpoint : server -> Endpoint.t

  type message =
    | Query of Sexp.t
    | Info of Sexp.t
    | Response of Sexp.t

  val send : client -> message -> (unit, string) Lwt_result.t

  val receive : client -> (message, string) Lwt_result.t
end

module MessageType = struct
  type query = Query

  type response = Response

  type info = Info
end

module type Message = sig
  val name : string

  val version : Semver.t

  module Address : Address.S

  type 'a t

  open MessageType

  val sexp_of_message : 'a t -> Sexp.t

  val query_of_sexp : Sexp.t -> (query t, string) Result.t

  val response_of_sexp : Sexp.t -> (response t, string) Result.t

  val info_of_sexp : Sexp.t -> (info t, string) Result.t
end

module type Transport = sig
  open MessageType

  module Message : Message

  module Wire : Wire

  type 'state server

  type client

  type endpoint = Wire.Endpoint.t

  type t

  val wire : t -> Wire.t

  val make : Message.Address.t -> t

  val connect : t -> endpoint -> (client, string) Lwt_result.t

  val serve :
    init:(endpoint -> ('state, string) Lwt_result.t) ->
    respond:
      ('state ->
      query Message.t ->
      ('state * response Message.t, string) Lwt_result.t) ->
    learn:('state -> info Message.t -> ('state, string) Lwt_result.t) ->
    t ->
    ('state server, string) Lwt_result.t

  val state :
    'state server ->
    ('state -> ('state * 'result, string) Lwt_result.t) ->
    ('result, string) Lwt_result.t

  val endpoint : _ server -> endpoint

  val send :
    client -> query Message.t -> (response Message.t, string) Lwt_result.t

  val inform : client -> info Message.t -> (unit, string) Lwt_result.t

  val wait : 'state server -> (unit, string) Lwt_result.t

  val stop : 'state server -> unit
end
