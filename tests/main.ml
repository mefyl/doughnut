open Core
module Lwt_utils = Dht.Lwt_utils
open Lwt_utils.O

module Address : Dht.Implementation.Address with type t = int = struct
  type t = int

  let compare = Stdlib.compare

  let sexp_of_t i = Sexp.Atom (string_of_int i)

  let t_of_sexp = function
    | Sexp.Atom s ->
        int_of_string s
    | _ ->
        failwith "invalid address"

  let random () = Random.int 255

  let space_log = 8

  let null = 0

  let log n =
    if n >= space_log then failwith "address log out of bound" else 1 lsl n

  let pp fmt addr = Format.pp_print_int fmt addr

  let to_string = string_of_int

  module O = struct
    let ( < ) = Stdlib.( < )

    let ( <= ) = Stdlib.( <= )

    let ( + ) l r = (l + r) mod 256
  end
end

module Transport = struct
  module Messages = Dht.Chord.Messages (Dht.Transport.DirectWire (Address))
  include Dht.Transport.DirectTransport (Address) (Messages)

  type filter = Passthrough | Response of Messages.response

  type stats = {mutable filtered: int}

  type t =
    { query_filter: stats -> Messages.query -> bool
    ; query: (Messages.query, filter) Lwt_utils.RPC.t
    ; stats: stats }

  let make_details query_filter =
    {query_filter; query= Lwt_utils.RPC.make (); stats= {filtered= 0}}

  let make () = make_details (fun _ _ -> false)

  let send t c m =
    if t.query_filter t.stats m then (
      t.stats.filtered <- t.stats.filtered + 1 ;
      Lwt_utils.RPC.send t.query m
      >>= function Passthrough -> send () c m | Response r -> Lwt.return r )
    else send () c m

  let connect _ = connect ()

  let listen _ = listen ()

  let endpoint _ = endpoint ()

  let receive _ = receive ()

  let respond _ = respond ()
end

module Dht =
  Dht.Chord.MakeDetails (Dht.Transport.DirectWire (Address)) (Transport)

let () =
  Logs.set_level (Some Logs.Debug) ;
  Logs.set_reporter (Logs.format_reporter ()) ;
  Logs_threaded.enable ()

open OUnit2

let generic_join _ =
  let main =
    let addresses = [0; 100; 200; 50; 250; 150] in
    let* endpoints, dhts =
      Lwt_utils.List.fold_map addresses ~init:[] ~f:(fun endpoints address ->
          let* dht = Dht.make address endpoints in
          Lwt.return (Dht.endpoint dht :: endpoints, dht))
    in
    List.iter
      ~f:(fun dht -> Logs.debug (fun m -> m "  dht: %a" Dht.pp_node dht.state))
      dhts ;
    ignore endpoints ;
    let* res = Dht.set (List.hd_exn dhts) 50 (Bytes.of_string "50") in
    Result.ok_or_failwith res ;
    let* res = Dht.set (List.hd_exn dhts) 150 (Bytes.of_string "150") in
    Result.ok_or_failwith res ;
    let* res = Dht.get (List.hd_exn dhts) 150 in
    OUnit.assert_equal ~printer:Bytes.to_string (Bytes.of_string "150")
      (Result.ok_or_failwith res) ;
    let+ res = Dht.get (List.hd_exn dhts) 50 in
    OUnit.assert_equal ~printer:Bytes.to_string (Bytes.of_string "50")
      (Result.ok_or_failwith res)
  in
  Lwt_main.run main

(* Test race conditions when joining the network.

* Create a network with nodes 0 and 100
* Make 10 and 20 join concurrently:
  * Let them both find their neighbors (0 and 100) and block hello queries.
  * Unlock one of them, which completes successfully.
  * Unlock the other which should fail

If `order`, unlock 10 first, verify 20 will not keep thinking 0 is its
predecessor and discover 10 actually is.

If `not order`, unlock 20 first and let it find 0 as a
predecessor. Check 10 discovers 100 is not its successor anymore, 20
is. *)

let chord_join_race order ctxt =
  let main =
    let* dht_pred = Dht.make 0 [] in
    let* dht_succ = Dht.make 100 [Dht.endpoint dht_pred] in
    let eps = [Dht.endpoint dht_pred; Dht.endpoint dht_succ] in
    let filter = function
      | {Transport.filtered} -> (
          function Transport.Messages.Hello _ -> filtered = 0 | _ -> false )
    in
    let transport_a = Transport.make_details filter
    and transport_b = Transport.make_details filter in
    let make_dht (transport, addr, pred) =
      let+ dht = Dht.make_details ~transport addr eps in
      OUnit2.assert_equal ~ctxt ~printer:Address.to_string pred
        (Dht.predecessor dht) ;
      dht
    in
    let dht_a = make_dht (transport_a, 10, 0)
    and dht_b = make_dht (transport_b, 20, if order then 10 else 0) in
    let* qa = Lwt_utils.RPC.receive transport_a.query
    and* qb = Lwt_utils.RPC.receive transport_b.query in
    let check_query s p = function
      | Transport.Messages.Hello {self= self, _; predecessor} ->
          let printer = Address.to_string in
          OUnit2.assert_equal ~ctxt ~printer s self ;
          OUnit2.assert_equal ~ctxt ~printer p predecessor
      | _ ->
          OUnit2.assert_string "wrong query"
    in
    check_query 10 0 qa ;
    check_query 20 0 qb ;
    let transport_a, dht_a, exp_a, transport_b, dht_b, exp_b =
      if order then (transport_a, dht_a, 0, transport_b, dht_b, 10)
      else (transport_b, dht_b, 10, transport_a, dht_a, 0)
    in
    let* () = Lwt_utils.RPC.respond transport_a.query Transport.Passthrough in
    let* dht_a = dht_a in
    let* () = Lwt_utils.RPC.respond transport_b.query Transport.Passthrough in
    let* dht_b = dht_b in
    let printer = Address.to_string in
    OUnit2.assert_equal ~ctxt ~printer exp_a (Dht.predecessor dht_a) ;
    OUnit2.assert_equal ~ctxt ~printer exp_b (Dht.predecessor dht_b) ;
    Lwt.return ()
  in
  Lwt_main.run main

let suite =
  "DHT"
  >::: [ "generic" >::: ["join" >:: generic_join]
       ; "chord"
         >::: [ "join"
                >::: [ "wrong_predecessor" >:: chord_join_race true
                     ; "wrong_successor" >:: chord_join_race false ] ] ]

let () = run_test_tt_main suite
