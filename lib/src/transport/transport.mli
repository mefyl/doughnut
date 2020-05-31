include module type of Transport_intf

module Direct () : Wire

(* module Tcp : Wire *)

module Make (W : Wire) (M : Message) :
  Transport with module Message = M and module Wire = W
