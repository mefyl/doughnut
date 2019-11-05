module O = struct
  let ( >>= ) = Lwt.bind

  let ( let* ) = ( >>= )

  let ( >>| ) p f = Lwt.bind p (fun v -> Lwt.return (f v))

  let ( let+ ) = ( >>| )

  let ( and* ) = Lwt.both
end

module O_result = struct
  let ( >>= ) = Lwt_result.bind

  let ( let* ) = ( >>= )

  let ( >>| ) p f = Lwt_result.bind p (fun v -> Lwt_result.return (f v))

  let ( let+ ) = ( >>| )

  let ( and* ) l r =
    let both = function
      | Result.Ok l, Result.Ok r ->
          Result.Ok (l, r)
      | (Result.Error _ as e), _ | _, (Result.Error _ as e) ->
          e
    in
    Lwt.bind l (fun l -> Lwt.bind r (fun r -> Lwt.return (both (l, r))))
end

module List = struct
  include Lwt_list
  open O_result

  let rec fold_map l ~init ~f =
    match l with
    | [] ->
        Lwt_result.return (init, [])
    | h :: t ->
        let* init, v = f init h in
        let+ res, t = fold_map t ~init ~f in
        (res, v :: t)
end

module RPC = struct
  type ('a, 'b) t =
    { send_stream: 'a Lwt_stream.t
    ; send: 'a option -> unit
    ; receive_stream: 'b Lwt_stream.t
    ; receive: 'b option -> unit }

  let make () =
    let send_stream, send = Lwt_stream.create ()
    and receive_stream, receive = Lwt_stream.create () in
    {send_stream; send; receive_stream; receive}

  let send {send; receive_stream; _} m =
    send (Some m) ;
    Lwt_stream.next receive_stream

  let receive {send_stream; _} = Lwt_stream.next send_stream

  let respond {receive; _} r =
    (* FIXME: sync ? *)
    Lwt.return (receive (Some r))
end

(* let 'a 'b split = Left of 'a | Right of 'b
 *
 * let poll a b =
 *   let open O in
 *   let a = Lwt.map (fun x -> Left x) a
 *   and b = Lwt.map (fun x -> Right x) b in
 *   Lwt.choose [a; b] >>= function
 *   | Left v -> *)
