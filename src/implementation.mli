open Base

module type Address = sig
  type t

  val compare : t -> t -> int

  val sexp_of : t -> Sexp.t

  val of_sexp : Sexp.t -> (t, string) Result.t

  val random : unit -> t

  val space : int

  val space_log : int

  val null : t

  val log : int -> t

  val pp : Formatter.t -> t -> unit

  val to_string : t -> string

  module O : sig
    val ( = ) : t -> t -> bool

    val ( < ) : t -> t -> bool

    val ( <= ) : t -> t -> bool

    val ( + ) : t -> t -> t

    val ( - ) : t -> t -> t
  end
end

module type Implementation = sig
  module Address : Address

  type node

  type endpoint

  val make : Address.t -> endpoint list -> (node, string) Lwt_result.t

  val endpoint : node -> endpoint

  val set : node -> Address.t -> Bytes.t -> (unit, string) Lwt_result.t

  val get : node -> Address.t -> (Bytes.t, string) Lwt_result.t
end
