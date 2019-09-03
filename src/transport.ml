module type Wire = sig
  module Address : Implementation.Address

  type ('a, 'b) client

  type ('a, 'b) server

  type ('a, 'b) endpoint

  type ('a, 'b) peer = Address.t * ('a, 'b) endpoint
end

module type Messages = sig
  type query

  type response
end

module type Transport = sig
  module Messages : Messages

  module Wire : Wire

  type client = (Messages.query, Messages.response) Wire.client

  type server = (Messages.query, Messages.response) Wire.server

  type endpoint = (Messages.query, Messages.response) Wire.endpoint

  type peer = (Messages.query, Messages.response) Wire.peer

  type t

  val make : unit -> t

  type id

  val connect : t -> endpoint -> client

  val listen : t -> unit -> server

  val endpoint : t -> server -> endpoint

  val send : t -> client -> Messages.query -> Messages.response Lwt.t

  val receive : t -> server -> (id * Messages.query) Lwt.t

  val respond : t -> server -> id -> Messages.response -> unit Lwt.t

  val pp_peer : Format.formatter -> peer -> unit
end

module DirectWire (A : Implementation.Address) = struct
  module Address = A

  type ('a, 'b) server = ('a, 'b) Lwt_utils.RPC.t

  and ('a, 'b) client = ('a, 'b) server

  and ('a, 'b) endpoint = ('a, 'b) server

  and ('a, 'b) peer = Address.t * ('a, 'b) endpoint
end

module DirectTransport (A : Implementation.Address) (Messages : Messages) =
struct
  type t = unit

  type id = unit

  module Wire = DirectWire (A)

  type client = (Messages.query, Messages.response) Wire.client

  type server = (Messages.query, Messages.response) Wire.server

  type endpoint = (Messages.query, Messages.response) Wire.endpoint

  type peer = (Messages.query, Messages.response) Wire.peer

  let make () = ()

  let connect _ e = e

  let listen _ () = Lwt_utils.RPC.make ()

  let endpoint _ s = s

  let send _ = Lwt_utils.RPC.send

  let receive _ rpc =
    Lwt.bind (Lwt_utils.RPC.receive rpc) (fun msg -> Lwt.return ((), msg))

  let respond _ rpc () resp = Lwt_utils.RPC.respond rpc resp

  let pp_peer fmt (addr, _) = A.pp fmt addr
end
