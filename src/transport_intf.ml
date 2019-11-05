open Core

module type Messages = sig
  type query

  val sexp_of_query : query -> Sexp.t

  val query_of_sexp : Sexp.t -> (query, string) Result.t

  type response

  val sexp_of_response : response -> Sexp.t

  val response_of_sexp : Sexp.t -> (response, string) Result.t

  type info

  val sexp_of_info : info -> Sexp.t

  val info_of_sexp : Sexp.t -> (info, string) Result.t
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
    t -> client -> Messages.query -> (Messages.response, string) Lwt_result.t

  val receive : t -> server -> (id * Messages.query) Lwt.t

  val respond : t -> server -> id -> Messages.response -> unit Lwt.t

  val inform : t -> client -> Messages.info -> unit
end
