open Base

module type S = sig
  type t

  val pp : Formatter.t -> t -> unit

  val to_string : t -> string

  val of_string : string -> (t, string) Result.t

  val to_sexp : t -> Sexp.t

  val of_sexp : Sexp.t -> (t, string) Result.t

  val compare : t -> t -> int
end
