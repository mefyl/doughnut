include module type of Transport_intf

module Direct : Wire with type t = unit

module Make (W : Wire) (M : Messages) :
  Transport with module Messages = M and module Wire = W
