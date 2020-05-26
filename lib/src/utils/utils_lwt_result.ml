(** Lwt_result converted to the Base monadic interface *)

include Lwt_result

include Base.Monad.Make2 (struct
  type nonrec ('t, 'error) t = ('t, 'error) Lwt_result.t

  let bind v ~f = Lwt_result.bind v f

  let map v ~f = Lwt_result.map f v

  let return = Lwt_result.return

  let map = `Custom map
end)
