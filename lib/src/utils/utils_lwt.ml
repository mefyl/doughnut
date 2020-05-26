(** Lwt converted to the Base monadic interface *)

include Lwt

include Base.Monad.Make (struct
  type nonrec 't t = 't Lwt.t

  let bind v ~f = Lwt.bind v f

  let map v ~f = Lwt.map f v

  let return = Lwt.return

  let map = `Custom map
end)
