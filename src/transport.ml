module type Types = sig
  module Address : Implementation.Address

  type client

  type server

  type endpoint

  type peer = Address.t * endpoint

  type message

  type response
end

module type Transport = sig
  module Types : Types

  type t

  val make : unit -> t

  type id

  val connect : t -> Types.endpoint -> Types.client

  val listen : t -> unit -> Types.server

  val endpoint : t -> Types.server -> Types.endpoint

  val send : t -> Types.client -> Types.message -> Types.response Lwt.t

  val receive : t -> Types.server -> (id * Types.message) Lwt.t

  val respond : t -> Types.server -> id -> Types.response -> unit Lwt.t

  val pp_peer : Format.formatter -> Types.peer -> unit
end
