open Utils

let lwt_ok v = Lwt.map ~f:Result.return v

let fail fmt = Format.kasprintf (fun m -> Lwt_result.fail m) fmt

module Csexp = struct
  include Csexp.Make (Sexp)

  module Parser = Make_parser (struct
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
end

type t = unit

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

type 'state server = {
  socket : Lwt_unix.file_descr;
  endpoint : Endpoint.t;
  state : 'state State.t;
}

open Let.Syntax2 (Lwt_result)

type client = {
  output : Lwt_io.output_channel;
  input : Lwt_io.input_channel;
  mutable rpc_count : int;
}

let make () = ()

let connect () ({ Endpoint.addr; port } as ep) =
  let+ input, output =
    try%lwt Lwt_io.open_connection (Unix.ADDR_INET (addr, port)) |> lwt_ok
    with Unix.Unix_error (Unix.ECONNREFUSED, _, _) ->
      fail "connection to %a refused" Endpoint.pp ep
  in
  { input; output; rpc_count = 0 }

type id = int

let convert_endpoint = function
  | Unix.ADDR_INET (addr, port) -> { Endpoint.addr; port }
  | _ -> failwith "unexpected Unix domain socket"

let serve ~init ~respond ~learn () =
  let open Let.Syntax (Lwt) in
  let socket = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  let* () =
    let address = Unix.ADDR_INET (Unix.inet_addr_of_string "0.0.0.0", 0) in
    Lwt_unix.bind socket address
  in
  let () = Lwt_unix.listen socket 64 in
  let endpoint = convert_endpoint @@ Lwt_unix.getsockname socket in
  let open Let.Syntax2 (Lwt_result) in
  let* state = init endpoint in
  let state = State.make state in
  let serve () =
    let serve_client socket addr () =
      let input = Lwt_io.of_fd ~mode:Lwt_io.input socket
      and output = Lwt_io.of_fd ~mode:Lwt_io.output socket in
      let rec loop () =
        Csexp.Parser.parse input >>= function
        | Sexp.List [ Sexp.Atom "query"; query ] ->
          let* response =
            let action state = respond state query in
            State.run state action
          in
          let* () = Lwt_io.write output @@ Csexp.to_string response |> lwt_ok in
          (loop [@tailcall]) ()
        | Sexp.List [ Sexp.Atom "info"; info ] ->
          let* () =
            let action state =
              let+ state = learn state info in
              (state, ())
            in
            State.run state action
          in
          (loop [@tailcall]) ()
        | e -> fail "invalid message: %a" Sexp.pp e
      in
      let open Lwt.Infix in
      loop () >>= function
      | Result.Ok () -> Lwt.return ()
      | Result.Error e ->
        Logs_lwt.warn ~src:Log.src (fun m ->
            m "client %a error: %s" Endpoint.pp addr e)
    in
    let rec accept () =
      let open Let.Syntax (Lwt) in
      let* client, addr = Lwt_unix.accept socket in
      let () = Lwt.async @@ serve_client client (convert_endpoint addr) in
      (accept [@tailcall]) ()
    in
    accept ()
  in
  let () = Lwt.async serve in
  Lwt_result.return { socket; endpoint; state }

let endpoint () { endpoint; _ } = endpoint

let send () client query =
  let read_rpc input =
    Csexp.Parser.parse input >>= function
    | Sexp.List [ Sexp.Atom id; response ] -> (
      match Int.of_string id with
      | id -> Lwt_result.return (id, response)
      | exception _ -> fail "invalid response id: %s" id )
    | r -> fail "invalid response: %a" Sexp.pp r
  in
  let rpc_id = client.rpc_count in
  let () = client.rpc_count <- client.rpc_count + 1 in
  let query = Sexp.List [ Sexp.Atom (Int.to_string rpc_id); query ] in
  let* () = Log.debug (fun m -> m "send %a" Sexp.pp query) in
  let* () = Lwt_io.write client.output (Csexp.to_string query) |> lwt_ok in
  let* id, response = read_rpc client.input in
  if id = rpc_id then
    Lwt_result.return response
  else
    fail "wrong response id: %i" id

let inform () client info =
  let* () = Log.debug (fun m -> m "send %a" Sexp.pp info) in
  Lwt_io.write client.output (Csexp.to_string info) |> lwt_ok

let state { state; _ } action = State.run state action

let stop { state; _ } = State.stop state

let wait { state; _ } =
  let+ _final_state = State.wait state in
  ()
