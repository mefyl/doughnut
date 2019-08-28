module Address = struct
  type t = int

  let random () = Random.int 255

  let space_log = 8

  let null = 0

  let log n =
    if n >= space_log then failwith "address log out of bound" else 1 lsl n

  let pp fmt addr = Format.pp_print_int fmt addr

  let to_string = string_of_int

  module O = struct
    let ( < ) = Stdlib.( < )

    let ( <= ) = Stdlib.( <= )

    let ( + ) l r = (l + r) mod 256
  end
end

module type Implementation = sig
  type node

  type endpoint

  val make : Address.t -> endpoint list -> node

  val endpoint : node -> endpoint
end
