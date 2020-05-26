type ('a, 'b) t = {
  send_stream : 'a Lwt_stream.t;
  send : 'a option -> unit;
  receive_stream : 'b Lwt_stream.t;
  receive : 'b option -> unit;
}

let make () =
  let send_stream, send = Lwt_stream.create ()
  and receive_stream, receive = Lwt_stream.create () in
  { send_stream; send; receive_stream; receive }

let send { send; receive_stream; _ } m =
  send (Some m);
  Lwt_stream.next receive_stream

let receive { send_stream; _ } = Lwt_stream.next send_stream

let respond { receive; _ } r =
  (* FIXME: sync ? *)
  Lwt.return (receive (Some r))
