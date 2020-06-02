open Base

module type S = sig
  module Address : Address.S

  type node

  type endpoint

  val make : Address.t -> endpoint list -> (node, string) Lwt_result.t

  val endpoint : node -> endpoint

  val wait : node -> (unit, string) Lwt_result.t

  val stop : node -> unit
end
