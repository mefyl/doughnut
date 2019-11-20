include Core.Option

let pp f fmt = function
  | None -> Format.pp_print_string fmt "none"
  | Some v ->
      Format.pp_print_string fmt "some ";
      f fmt v
