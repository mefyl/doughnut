open Utils
include Address_intf

module Int64 = struct
  type t = Int64.t

  let length = 8

  let compare = Int64.compare

  let to_string = Int64.Hex.to_string

  let of_string r =
    try Result.return @@ Int64.Hex.of_string r
    with _ -> Result.fail "invalid address"

  let sexp_of a = Sexp.Atom (to_string a)

  let pp fmt a = Format.fprintf fmt "%Lx" a

  let of_sexp = function
    | Sexp.Atom a -> of_string a
    | _ -> Result.fail "invalid address"

  let space_log = length * 8

  let null = 0L

  let log n = Int64.shift_left 0L n

  module O = Int64.O
end
