open Core

module type Address = sig
  type t

  val compare : t -> t -> int

  val sexp_of_t : t -> Sexp.t

  val t_of_sexp : Sexp.t -> t

  val random : unit -> t

  val space_log : int

  val null : t

  val log : int -> t

  val pp : Format.formatter -> t -> unit

  val to_string : t -> string

  module O : sig
    val ( < ) : t -> t -> bool

    val ( <= ) : t -> t -> bool

    val ( + ) : t -> t -> t
  end
end

module type Implementation = sig
  module Address : Address

  type node

  type endpoint

  val make : Address.t -> endpoint list -> node Lwt.t

  val endpoint : node -> endpoint

  val set : node -> Address.t -> Bytes.t -> (unit, string) Result.t Lwt.t

  val get : node -> Address.t -> (Bytes.t, string) Result.t Lwt.t
end
