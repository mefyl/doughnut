open Utils

module Csexp = struct
  include Csexp.Make (Sexp)

  module Parser = struct
    include Make_parser (struct
      open Let.Syntax2 (Lwt_result)

      type t = Lwt_io.input_channel

      module Monad = struct
        type 'a t = 'a Lwt.t

        let return = Lwt.return

        let bind v f = Lwt.bind ~f v
      end

      let read_string input count =
        let res = Bytes.create count in
        let rec read pos =
          let* size = Lwt_io.read_into input res pos (count - pos) |> lwt_ok in
          if size = 0 then
            Lwt_result.fail "premature end of input"
          else
            let pos = pos + size in
            if pos < count then
              read pos
            else
              Lwt_result.return ()
        in
        let+ () = read 0 in
        Bytes.unsafe_to_string ~no_mutation_while_string_reachable:res

      let read_char input =
        try Lwt_io.read_char input |> lwt_ok
        with End_of_file -> Lwt_result.fail "premature end of input"
    end)

    let parse input =
      try%lwt parse input
      with End_of_file -> Lwt_result.fail "connection closed by peer"
  end
end

type t = unit

type message =
  | Query of Sexp.t
  | Info of Sexp.t
  | Response of Sexp.t

module Endpoint = struct
  type t = {
    addr : Unix.inet_addr;
    port : int;
  }

  let compare l r =
    (* FIXME: not great, but AFAICT we have no other interpretation possible of
       Unix.inet_addr. *)
    let cmp =
      String.compare
        (Unix.string_of_inet_addr l.addr)
        (Unix.string_of_inet_addr r.addr)
    in
    if cmp <> 0 then
      cmp
    else
      l.port - r.port

  let pp fmt { addr; port } =
    Format.fprintf fmt "%s:%i" (Unix.string_of_inet_addr addr) port

  let to_string { addr; port } =
    Fmt.str "%s:%i" (Unix.string_of_inet_addr addr) port

  let of_string s =
    match String.split ~on:':' s with
    | [ addr; port ] ->
      let open Let.Syntax2 (Result) in
      let+ addr =
        try Result.return @@ Unix.inet_addr_of_string addr
        with Failure _ -> Result.failf "invalid IP address: %s" addr
      and+ port =
        try Result.return @@ Int.of_string port
        with Failure _ -> Result.failf "invalid port: %s" port
      in
      { addr; port }
    | _ -> Result.failf "invalid TCP endpoint: %s" s

  let to_sexp { addr; port } =
    Sexp.List
      [
        Sexp.Atom (Unix.string_of_inet_addr addr);
        Sexp.Atom (Int.to_string port);
      ]

  let of_sexp = function
    | Sexp.List [ Sexp.Atom addr; Sexp.Atom port ] -> (
      match Caml.int_of_string_opt port with
      | Some port -> (
        try Result.return { addr = Unix.inet_addr_of_string addr; port }
        with _ -> Result.Error ("invalid network address: " ^ addr) )
      | None -> Result.Error ("invalid port: " ^ port) )
    | sexp -> Result.Error (Format.asprintf "invalid endpoint: %a" Sexp.pp sexp)
end

type server = {
  socket : Lwt_unix.file_descr;
  endpoint : Endpoint.t;
  ended : unit Lwt.t;
  end_ : unit Lwt.u;
  stopped : unit Lwt.t;
  stop : unit Lwt.u;
}

open Let.Syntax2 (Lwt_result)

type client = {
  output : Lwt_io.output_channel;
  input : Lwt_io.input_channel;
}

let make () = ()

let connect () ({ Endpoint.addr; port } as ep) =
  let+ input, output =
    try%lwt Lwt_io.open_connection (Unix.ADDR_INET (addr, port)) |> lwt_ok
    with Unix.Unix_error (Unix.ECONNREFUSED, _, _) ->
      fail "connection to %a refused" Endpoint.pp ep
  in
  { input; output }

type id = int

let convert_endpoint = function
  | Unix.ADDR_INET (addr, port) -> { Endpoint.addr; port }
  | _ -> failwith "unexpected Unix domain socket"

let bind () =
  let open Let.Syntax (Lwt) in
  let socket = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  let* () =
    let address = Unix.ADDR_INET (Unix.inet_addr_of_string "0.0.0.0", 0) in
    Lwt_unix.bind socket address
  in
  let endpoint = convert_endpoint @@ Lwt_unix.getsockname socket
  and stopped, stop = Lwt.wait ()
  and ended, end_ = Lwt.wait () in
  Lwt_result.return { socket; endpoint; stopped; stop; ended; end_ }

let alternative a b =
  Lwt.choose [ Lwt.map ~f:Either.first a; Lwt.map ~f:Either.second b ]

let serve server f =
  let open Let.Syntax (Lwt) in
  let f client () =
    let* res = f client in
    result_warn () res "client error"
  in
  let rec accept () =
    alternative (Lwt_unix.accept server.socket) server.stopped >>= function
    | Either.First (socket, _) ->
      let client =
        {
          input = Lwt_io.of_fd ~mode:Lwt_io.input socket;
          output = Lwt_io.of_fd ~mode:Lwt_io.output socket;
        }
      in
      let () = Lwt.async @@ f client in
      (accept [@tailcall]) ()
    | Either.Second () ->
      (* FIXME: close all clients properly etc *)
      let+ () = Lwt_unix.close server.socket in
      Lwt.wakeup server.end_ ()
  in
  Lwt.async accept

let endpoint { endpoint; _ } = endpoint

let send { output; _ } message =
  let message =
    let kind, message =
      match message with
      | Query m -> ("query", m)
      | Info m -> ("info", m)
      | Response m -> ("response", m)
    in
    Sexp.List [ Sexp.Atom kind; message ]
  in
  let* () = Log.debug (fun m -> m "send: %a" Sexp.pp message) in
  Lwt_io.write output (Csexp.to_string message) |> lwt_ok

let receive { input; _ } =
  Csexp.Parser.parse input >>= function
  | Sexp.List [ Sexp.Atom kind; message ] -> (
    match kind with
    | "query" -> Lwt_result.return @@ Query message
    | "info" -> Lwt_result.return @@ Info message
    | "response" -> Lwt_result.return @@ Response message
    | s -> fail "invalid message type: %S" s )
  | sexp -> fail "invalid message: %a" Sexp.pp sexp

let stop { stop; _ } = Lwt.wakeup stop ()

let wait { ended; _ } = ended |> lwt_ok
