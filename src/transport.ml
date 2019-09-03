module type Types = sig
  module Address : Implementation.Address

  type client

  type server

  type endpoint

  type peer = Address.t * endpoint

  type message

  type response
end

module type Transport = sig
  module Types : Types

  type t

  val make : unit -> t

  type id

  val connect : t -> Types.endpoint -> Types.client

  val listen : t -> unit -> Types.server

  val endpoint : t -> Types.server -> Types.endpoint

  val send : t -> Types.client -> Types.message -> Types.response Lwt.t

  val receive : t -> Types.server -> (id * Types.message) Lwt.t

  val respond : t -> Types.server -> id -> Types.response -> unit Lwt.t

  val pp_peer : Format.formatter -> Types.peer -> unit
end

module type DirectTypes = sig
  module Address : Implementation.Address

  type message

  type response

  type server = (message, response) Lwt_utils.RPC.t

  type client = server

  type endpoint = server

  type peer = Address.t * endpoint
end

module DirectTransport (A : Implementation.Address) (Types : DirectTypes) =
struct
  type t = unit

  type id = unit

  let make () = ()

  let connect _ e = e

  let listen _ () = Lwt_utils.RPC.make ()

  let endpoint _ s = s

  let send _ = Lwt_utils.RPC.send

  let receive _ rpc =
    Lwt.bind (Lwt_utils.RPC.receive rpc) (fun msg -> Lwt.return ((), msg))

  let respond _ rpc () resp = Lwt_utils.RPC.respond rpc resp

  let pp_peer fmt (addr, _) = Types.Address.pp fmt addr
end
