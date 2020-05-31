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

type server = {
  fd : Unix.file_descr;
  server : Lwt_io.server;
}

open Let.Syntax2 (Lwt_result)

type client = {
  output : Lwt_io.output_channel;
  input : Lwt_io.input_channel;
  mutable rpc_count : int;
}

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

type t = unit

let make () = ()

let connect () { Endpoint.addr; port } =
  let+ input, output =
    Lwt_io.open_connection (Unix.ADDR_INET (addr, port)) |> lwt_ok
  in
  { input; output; rpc_count = 0 }

type id = int

let bind () =
  let handler _ _ = Lwt.return () in
  let fd = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  let+ server =
    Lwt_io.establish_server_with_client_address
      ~fd:(Lwt_unix.of_unix_file_descr fd)
      (Unix.ADDR_INET (Unix.inet_addr_of_string "0.0.0.0", 0))
      handler
    |> lwt_ok
  in
  { fd; server }

let listen _ ~respond:_ ~learn:_ _ = failwith "FILLME"

let endpoint () { fd; _ } =
  match Unix.getsockname fd with
  | Unix.ADDR_INET (addr, port) -> { Endpoint.addr; port }
  | _ -> failwith "unexpected Unix domain socket"

let read_rpc input =
  Csexp.Parser.parse input >>= function
  | Sexp.List [ Sexp.Atom id; response ] -> (
    match Int.of_string id with
    | id -> Lwt_result.return (id, response)
    | exception _ -> fail "invalid response id: %s" id )
  | r -> fail "invalid response: %a" Sexp.pp r

let send () client query =
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
