(** Result converted to the Base monadic interface *)

include Base.Result

include Base.Monad.Make2 (struct
  type nonrec ('t, 'error) t = ('t, 'error) Result.t

  let bind v ~f = bind ~f v

  let map v ~f = map ~f v

  let return = return

  let map = `Custom map
end)
