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

module Make (W : Wire) (M : Messages) : sig
  module Wire : Wire

  type client

  type server

  type endpoint

  type peer
end

module type Transport = sig
  module Messages : Messages

  module Wire : Wire

  type client = (Messages.query, Messages.response) Wire.client

  type server = (Messages.query, Messages.response) Wire.server

  type endpoint = (Messages.query, Messages.response) Wire.endpoint

  type peer = (Messages.query, Messages.response) Wire.peer

  type t

  type id

  val make : unit -> t

  val connect : t -> endpoint -> client

  val listen : t -> unit -> server

  val endpoint : t -> server -> endpoint

  val send : t -> client -> Messages.query -> Messages.response Lwt.t

  val receive : t -> server -> (id * Messages.query) Lwt.t

  val respond : t -> server -> id -> Messages.response -> unit Lwt.t

  val pp_peer : Format.formatter -> peer -> unit
end

module Direct : sig
  module Wire (A : Implementation.Address) : Wire with type Address.t = A.t

  module Transport (A : Implementation.Address) (Messages : Messages) : sig
    module Wire : module type of Wire (A)

    type client = (Messages.query, Messages.response) Wire.client

    type server = (Messages.query, Messages.response) Wire.server

    type endpoint = (Messages.query, Messages.response) Wire.endpoint

    type peer = (Messages.query, Messages.response) Wire.peer

    type t = unit

    type id = unit

    val make : unit -> t

    val connect : t -> endpoint -> client

    val listen : t -> unit -> server

    val endpoint : t -> server -> endpoint

    val send : t -> client -> Messages.query -> Messages.response Lwt.t

    val receive : t -> server -> (id * Messages.query) Lwt.t

    val respond : t -> server -> id -> Messages.response -> unit Lwt.t

    val pp_peer : Format.formatter -> peer -> unit
  end
end
