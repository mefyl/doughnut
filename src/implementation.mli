module type Address = sig
  type t

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

  val make : Address.t -> endpoint list -> node

  val endpoint : node -> endpoint
end
