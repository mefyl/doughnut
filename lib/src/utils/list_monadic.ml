module Make2 (Monad : Base.Monad.S2) = struct
  include Base.List

  let map ~f l =
    let open Monad in
    fold_right l ~init:(Monad.return []) ~f:(fun l acc ->
        f l >>= fun v ->
        acc >>= fun acc -> Monad.return (v :: acc))

  let rec fold_map l ~init ~f =
    let open Let.Syntax2 (Monad) in
    match l with
    | [] -> Monad.return (init, [])
    | h :: t ->
      let* init, v = f init h in
      let+ res, t = fold_map t ~init ~f in
      (res, v :: t)
end

module Make (Monad : Base.Monad.S) = struct
  include Make2 (struct
    include Monad

    type nonrec ('a, 'b) t = 'a t
  end)
end
