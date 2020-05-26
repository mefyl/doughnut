open Doughnut
open Utils

open Let.Syntax2 (Lwt_result)

module Format = Caml.Format

module Address : Doughnut.Address.S with type t = int = struct
  type t = int

  let compare = Stdlib.compare

  let sexp_of i = Sexp.Atom (Int.to_string i)

  let of_sexp = function
    | Sexp.Atom s -> (
      try Result.Ok (Int.of_string s)
      with Failure _ -> Result.Error ("invalid integer address: " ^ s) )
    | sexp -> Result.Error (Format.asprintf "invalid address: %a" Sexp.pp sexp)

  let random () = Random.int 255

  let space = 256

  let space_log = 8

  let null = 0

  let log n =
    if n >= space_log then
      failwith "address log out of bound"
    else
      1 lsl n

  let pp fmt addr = Format.pp_print_int fmt addr

  let to_string = Int.to_string

  module O = struct
    let ( = ) = Stdlib.( = )

    let ( < ) = Stdlib.( < )

    let ( <= ) = Stdlib.( <= )

    let ( + ) l r = (l + r) % 256

    let ( - ) l r =
      let res = (l - r) % 256 in
      if res < 0 then
        res + 256
      else
        res
  end
end

module ChordMessages =
  Doughnut.Chord.Messages (Address) (Doughnut.Transport.Direct)
open Doughnut.Transport.MessagesType

module Transport = struct
  include Doughnut.Transport.Make (Doughnut.Transport.Direct) (ChordMessages)

  type filter =
    | Passthrough
    | Response of response ChordMessages.message

  type stats = { mutable filtered : int }

  type wrapped = t

  type t = {
    wrapped : wrapped;
    query_filter : stats -> query ChordMessages.message -> bool;
    query : (query ChordMessages.message, filter) Rpc.t;
    stats : stats;
  }

  let wire t = wire t.wrapped

  let make_details query_filter =
    {
      wrapped = make ();
      query_filter;
      query = Rpc.make ();
      stats = { filtered = 0 };
    }

  let make () = make_details (fun _ _ -> false)

  let connect t = connect t.wrapped

  let listen t = listen t.wrapped

  let endpoint t = endpoint t.wrapped

  let send t c m =
    if t.query_filter t.stats m then (
      t.stats.filtered <- t.stats.filtered + 1;
      Lwt.map ~f:Result.return (Rpc.send t.query m) >>= function
      | Passthrough -> send t.wrapped c m
      | Response r -> Lwt_result.return r
    ) else
      send t.wrapped c m

  let receive t = receive t.wrapped

  let respond t = respond t.wrapped

  let inform t = inform t.wrapped

  let learn t = learn t.wrapped
end

module Dht =
  Doughnut.Chord.MakeDetails (Address) (Doughnut.Transport.Direct) (Transport)

let () =
  Logs.set_level (Some Logs.Debug);
  Logs.set_reporter (Logs.format_reporter ());
  Logs_threaded.enable ()

open OUnit2

let generic_single _ =
  let main =
    let+ dht = Dht.make 0 [] in
    ignore dht
  in
  Result.ok_or_failwith (Lwt_main.run main)

let generic_join _ =
  let main =
    let addresses = [ 0; 100; 200; 50; 250; 150 ] in
    let open Let.Syntax (Lwt) in
    let* dhts =
      let open Let.Syntax2 (Lwt_result) in
      let module List = List_monadic.Make2 (Lwt_result) in
      List.fold_map addresses ~init:[] ~f:(fun endpoints address ->
          let* dht = Dht.make address endpoints in
          Lwt_result.return (Dht.endpoint dht :: endpoints, dht))
    in
    let endpoints, dhts = Result.ok_or_failwith dhts in
    List.iter
      ~f:(fun dht -> Logs.debug (fun m -> m "  dht: %a" Dht.pp_node dht.state))
      dhts;
    ignore endpoints;
    let* res = Dht.set (List.hd_exn dhts) 50 (Bytes.of_string "50") in
    Result.ok_or_failwith res;
    let* res = Dht.set (List.hd_exn dhts) 150 (Bytes.of_string "150") in
    Result.ok_or_failwith res;
    let* res = Dht.get (List.hd_exn dhts) 150 in
    OUnit.assert_equal ~printer:Bytes.to_string (Bytes.of_string "150")
      (Result.ok_or_failwith res);
    let+ res = Dht.get (List.hd_exn dhts) 50 in
    OUnit.assert_equal ~printer:Bytes.to_string (Bytes.of_string "50")
      (Result.ok_or_failwith res)
  in
  Lwt_main.run main

(* Test race conditions when joining the network.

   * Create a network with nodes 0 and 100 * Make 10 and 20 join concurrently: *
   Let them both find their neighbors (0 and 100) and block hello queries. *
   Unlock one of them, which completes successfully. * Unlock the other which
   should fail

   If `order`, unlock 10 first, verify 20 will not keep thinking 0 is its
   predecessor and discover 10 actually is.

   If `not order`, unlock 20 first and let it find 0 as a predecessor. Check 10
   discovers 100 is not its successor anymore, 20 is. *)

let chord_join_race order ctxt =
  let main =
    let* dht_pred = Dht.make 0 [] in
    let* dht_succ = Dht.make 100 [ Dht.endpoint dht_pred ] in
    let eps = [ Dht.endpoint dht_pred; Dht.endpoint dht_succ ] in
    let filter = function
      | { Transport.filtered } -> (
        function
        | ChordMessages.Hello _ -> filtered = 0
        | _ -> false )
    in
    let transport_a = Transport.make_details filter
    and transport_b = Transport.make_details filter in
    let make_dht (transport, addr, pred) =
      let+ dht = Dht.make_details ~transport addr eps in
      OUnit2.assert_equal ~ctxt ~printer:Address.to_string pred
        (Dht.predecessor dht);
      dht
    in
    let dht_a = make_dht (transport_a, 10, 0)
    and dht_b =
      make_dht
        ( transport_b,
          20,
          if order then
            10
          else
            0 )
    in
    let* qa = Lwt.map ~f:Result.return (Rpc.receive transport_a.query)
    and* qb = Lwt.map ~f:Result.return (Rpc.receive transport_b.query) in
    let check_query s p = function
      | ChordMessages.Hello { self; predecessor } ->
        let printer = Address.to_string in
        OUnit2.assert_equal ~ctxt ~printer s self.address;
        OUnit2.assert_equal ~ctxt ~printer p predecessor
      | _ -> OUnit2.assert_string "wrong query"
    in
    check_query 10 0 qa;
    check_query 20 0 qb;
    let transport_a, dht_a, exp_a, transport_b, dht_b, exp_b =
      if order then
        (transport_a, dht_a, 0, transport_b, dht_b, 10)
      else
        (transport_b, dht_b, 10, transport_a, dht_a, 0)
    in
    let open Let.Syntax (Lwt) in
    let* () = Rpc.respond transport_a.query Transport.Passthrough in
    let open Let.Syntax2 (Lwt_result) in
    let* dht_a = dht_a in
    let open Let.Syntax (Lwt) in
    let* () = Rpc.respond transport_b.query Transport.Passthrough in
    let open Let.Syntax2 (Lwt_result) in
    let* dht_b = dht_b in
    let printer = Address.to_string in
    OUnit2.assert_equal ~ctxt ~printer exp_a (Dht.predecessor dht_a);
    OUnit2.assert_equal ~ctxt ~printer exp_b (Dht.predecessor dht_b);
    Lwt_result.return ()
  in
  Result.ok_or_failwith (Lwt_main.run main)

let assert_greater ?ctxt ?(cmp = fun a b -> a > b) ?printer ?pp_diff ?msg a b =
  let printer =
    let f printer v =
      if v = a then
        "at most " ^ printer v
      else
        printer v
    in
    Option.map printer ~f
  in
  OUnit2.assert_equal ?ctxt ~cmp ?printer ?pp_diff ?msg a b

let assert_less ?ctxt ?(cmp = fun a b -> a < b) ?printer ?pp_diff ?msg a b =
  let printer =
    let f printer v =
      if v = a then
        "at least " ^ printer v
      else
        printer v
    in
    Option.map printer ~f
  in
  OUnit2.assert_equal ?ctxt ~cmp ?printer ?pp_diff ?msg a b

let chord_fix_index_overshoot ctxt =
  let count = ref 0 in
  let make_details =
    let filter _ = function
      | ChordMessages.Successor _ ->
        count := !count + 1;
        false
      | _ -> false
    in
    let transport = Transport.make_details filter in
    Dht.make_details ~transport
  in
  let main =
    let printer = Int.to_string in
    (* Setup so dht_0 wrongfully thinks dht_2 is its successor. *)
    let* dht_0 = make_details 0 [] in
    let* dht_2 = make_details 2 [ Dht.endpoint dht_0 ] in
    let* dht_1 = make_details 1 [ Dht.endpoint dht_2 ] in
    let* () = Dht.set dht_1 1 (Bytes.of_string "") in
    let before = !count in
    let* _ = Dht.get dht_0 1 in
    (* The first run must fix the finger table. *)
    assert_less ~ctxt ~printer (before + 1) !count;
    let before = !count in
    let* _ = Dht.get dht_0 1 in
    (* The second run must find the value in one hop. *)
    OUnit2.assert_equal ~ctxt ~printer (before + 1) !count;
    Lwt_result.return ()
  in
  Result.ok_or_failwith (Lwt_main.run main)

let chord_complexity ctxt =
  let main =
    let rec enumerate acc = function
      | 0 -> 0 :: acc
      | n -> enumerate (n :: acc) (n - 1)
    in
    let addresses = enumerate [] ((Address.space - 1) / 10) in
    let count = ref 0 in
    let* endpoints, dhts =
      let module List = List_monadic.Make2 (Lwt_result) in
      List.fold_map addresses ~init:[] ~f:(fun endpoints address ->
          let filter _ = function
            | ChordMessages.Successor _ ->
              count := !count + 1;
              false
            | _ -> false
          in
          let transport = Transport.make_details filter in
          let* dht = Dht.make_details ~transport address endpoints in
          Lwt_result.return (Dht.endpoint dht :: endpoints, dht))
    in
    ignore endpoints;
    ignore dhts;
    assert_greater ~ctxt ~printer:Int.to_string 50 !count;
    Lwt_result.return ()
  in
  Result.ok_or_failwith (Lwt_main.run main)

let suite =
  "DHT"
  >::: [
         "generic" >::: [ "join" >:: generic_join; "single" >:: generic_single ];
         "chord"
         >::: [
                "join"
                >::: [
                       "wrong_predecessor" >:: chord_join_race true;
                       "wrong_successor" >:: chord_join_race false;
                     ];
                "lookup"
                >::: [
                       "index_overshot" >:: chord_fix_index_overshoot;
                       "complexity" >:: chord_complexity;
                     ];
              ];
       ]

let () = run_test_tt_main suite
