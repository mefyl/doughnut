open Base

module type S = sig
  include Mesh.S

  val set : node -> Address.t -> Bytes.t -> (unit, string) Lwt_result.t

  val get : node -> Address.t -> (Bytes.t, string) Lwt_result.t
end
