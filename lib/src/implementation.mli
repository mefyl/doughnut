open Base

module type Implementation = sig
  module Address : Address.S

  type node

  type endpoint

  val make : Address.t -> endpoint list -> (node, string) Lwt_result.t

  val endpoint : node -> endpoint

  val set : node -> Address.t -> Bytes.t -> (unit, string) Lwt_result.t

  val get : node -> Address.t -> (Bytes.t, string) Lwt_result.t
end
