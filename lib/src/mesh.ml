open Base

module type S = sig
  module Address : Address.S

  module Transport : Transport.Transport

  type node

  type endpoint = Transport.endpoint

  val make : Address.t -> endpoint list -> (node, string) Lwt_result.t

  val endpoint : node -> endpoint

  val wait : node -> (unit, string) Lwt_result.t

  val stop : node -> unit
end
