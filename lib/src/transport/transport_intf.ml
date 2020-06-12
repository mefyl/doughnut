open Base

module type Wire = sig
  type t

  type client

  type server

  module Endpoint : Endpoint.S

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

  type t

  type 'state server

  type 'state client

  val make : unit -> t

  val serve :
    t ->
    address:Message.Address.t ->
    init:(unit server -> ('state, string) Lwt_result.t) ->
    respond:
      ('state server ->
      query Message.t ->
      (response Message.t, string) Lwt_result.t) ->
    learn:('state server -> info Message.t -> (unit, string) Lwt_result.t) ->
    ('state server, string) Lwt_result.t

  val connect :
    'state server -> Wire.Endpoint.t -> ('state client, string) Lwt_result.t

  val state :
    'state server ->
    ('state -> ('state * 'result, string) Lwt_result.t) ->
    ('result, string) Lwt_result.t

  val address : 'state server -> Message.Address.t

  val endpoint : 'state server -> Wire.Endpoint.t

  val send :
    'state client ->
    query Message.t ->
    (response Message.t, string) Lwt_result.t

  val inform : 'state client -> info Message.t -> (unit, string) Lwt_result.t

  val wait : 'state server -> (unit, string) Lwt_result.t

  val stop : 'state server -> unit
end
