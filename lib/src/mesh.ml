open Base

module type S = sig
  module Address : Address.S

  module Transport : Transport.Transport

  module Endpoint = Transport.Wire.Endpoint

  type node

  val make :
    Address.t ->
    ?started:(Transport.Wire.Endpoint.t -> (unit, string) Lwt_result.t) ->
    Endpoint.t list ->
    (node, string) Lwt_result.t

  val endpoint : node -> Endpoint.t

  val wait : node -> (unit, string) Lwt_result.t

  val stop : node -> unit
end
