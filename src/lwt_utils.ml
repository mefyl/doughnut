module O = struct
  let ( >>= ) = Lwt.bind

  let ( let* ) = ( >>= )

  let ( >>| ) p f = Lwt.bind p (fun v -> Lwt.return (f v))

  let ( let+ ) = ( >>| )

  let ( and* ) = Lwt.both
end

module List = struct
  include Lwt_list
  open O

  let rec fold_map l ~init ~f =
    match l with
    | [] ->
        Lwt.return (init, [])
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
