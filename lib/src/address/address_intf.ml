open Base

(** Block and node addresses.

    Every block and node is attributed a unique address. The size of the address
    space, ie the number of possible different addresses, must be a power of 2. *)
module type S = sig
  (** An address *)
  type t

  (** Global ordering over adresses *)
  val compare : t -> t -> int

  (** A special, null address *)
  val null : t

  (** Log2 of the address space size. *)
  val space_log : int

  (** [log n] is the address it possition [2 ** n]. *)
  val log : int -> t

  (** String representation *)
  val to_string : t -> string

  (** String parsing *)
  val of_string : string -> (t, string) Result.t

  (** Sexp conversion *)
  val sexp_of : t -> Sexp.t

  (** Sexp conversion *)
  val of_sexp : Sexp.t -> (t, string) Result.t

  (** Pretty print *)
  val pp : Formatter.t -> t -> unit

  module O : sig
    val ( = ) : t -> t -> bool

    val ( < ) : t -> t -> bool

    val ( <= ) : t -> t -> bool

    val ( + ) : t -> t -> t

    val ( - ) : t -> t -> t
  end
end
