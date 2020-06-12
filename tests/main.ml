open Doughnut
open Utils

open Let.Syntax2 (Lwt_result)

module Format = Caml.Format

module Address : Doughnut.Address.S with type t = int = struct
  module T = struct
    type t = int

    let compare = Stdlib.compare

    let to_string = Int.to_string

    let of_string s =
      try Result.Ok (Int.of_string s)
      with Failure _ -> Result.Error ("invalid integer address: " ^ s)

    let sexp_of i = Sexp.Atom (Int.to_string i)

    let sexp_of_t = sexp_of

    let of_sexp = function
      | Sexp.Atom s -> of_string s
      | sexp ->
        Result.Error (Format.asprintf "invalid address: %a" Sexp.pp sexp)

    let space_log = 8

    let null = 0

    let log n =
      if n >= space_log then
        failwith "address log out of bound"
      else
        1 lsl n

    let pp fmt addr = Format.pp_print_int fmt addr

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

  include T
  include Comparator.Make (T)
end

module Alcotest = struct
  include Alcotest

  let bytes =
    Alcotest.testable
      (fun fmt b ->
        Format.fprintf fmt "%S"
          (Bytes.unsafe_to_string ~no_mutation_while_string_reachable:b))
      Bytes.( = )

  let address = Alcotest.testable Address.pp Address.O.( = )

  let int_greater = Alcotest.testable Int.pp ( > )

  let int_less = Alcotest.testable Int.pp ( < )
end

module Wire = Doughnut.Transport.Direct ()

module ChordMessage = Doughnut.Chord.Message (Address) (Wire)
open Doughnut.Transport.MessageType

module Transport = struct
  include Doughnut.Transport.Make (Wire) (ChordMessage)

  type filter =
    | Passthrough
    | Response of response ChordMessage.t

  type stats = { mutable filtered : int }

  type wrapped = t

  type t = {
    wrapped : wrapped;
    query_filter : stats -> query ChordMessage.t -> bool;
    query : (query ChordMessage.t, filter) Rpc.t;
    stats : stats;
  }

  type nonrec 'state server = {
    server : 'state server;
    t : t;
  }

  type nonrec 'state client = {
    client : 'state client;
    t : t;
  }

  let make_details query_filter =
    let stats = { filtered = 0 }
    and query = Rpc.make ()
    and wrapped = make () in
    { wrapped; query_filter; query; stats }

  let make () = make_details (fun _ _ -> false)

  let send { client; t } m =
    if t.query_filter t.stats m then (
      t.stats.filtered <- t.stats.filtered + 1;
      Lwt.map ~f:Result.return (Rpc.send t.query m) >>= function
      | Passthrough -> send client m
      | Response r -> Lwt_result.return r
    ) else
      send client m

  let inform client = inform client.client

  let serve ({ wrapped; _ } as t) ~address ~init ~respond ~learn =
    let init server = init { server; t }
    and respond server = respond { server; t }
    and learn server = learn { server; t } in
    let+ server = serve wrapped ~address ~init ~respond ~learn in
    { server; t }

  let connect { server; t } endpoint =
    let+ client = connect server endpoint in
    { client; t }

  let address { server; _ } = address server

  let endpoint { server; _ } = endpoint server

  let state { server; _ } = state server

  let stop { server; _ } = stop server

  let wait { server; _ } = wait server
end

module Dht = Doughnut.Chord.MakeDetails (Address) (Wire) (Transport)

let () =
  Logs.set_level ~all:true (Some Logs.Debug);
  Logs.set_reporter (Logs.format_reporter ());
  Logs_threaded.enable ()

let generic_single _ =
  let+ _ = Dht.make 0 [] in
  ()

let generic_join _ =
  let module Lwt_list = List_monadic.Make2 (Lwt_result) in
  let addresses = [ 0; 100; 200; 50; 250; 150 ] in
  let open Let.Syntax2 (Lwt_result) in
  let* endpoints, dhts =
    let f endpoints address =
      let* () =
        Logs_lwt.info (fun m -> m "create DTH %a" Address.pp address) |> lwt_ok
      in
      let* dht = Dht.make address endpoints in
      Lwt_result.return (Dht.endpoint dht :: endpoints, dht)
    in
    Lwt_list.fold_map ~init:[] ~f addresses
  in
  let f dht =
    Logs_lwt.debug (fun m -> m "  dht: %a" Dht.pp_node dht) |> lwt_ok
  in
  let* () = Lwt_list.iter ~f dhts in
  ignore endpoints;
  let* () = Dht.set (List.hd_exn dhts) 50 (Bytes.of_string "50") in
  let* () = Dht.set (List.hd_exn dhts) 150 (Bytes.of_string "150") in
  let* res = Dht.get (List.hd_exn dhts) 150 in
  Alcotest.(check bytes "retrieved block" (Bytes.of_string "150") res);
  let+ res = Dht.get (List.hd_exn dhts) 50 in
  Alcotest.(check bytes "retrieved block" (Bytes.of_string "50") res)

(* Test race conditions when joining the network.

   * Create a network with nodes 0 and 100 * Make 10 and 20 join concurrently: *
   Let them both find their neighbors (0 and 100) and block hello queries. *
   Unlock one of them, which completes successfully. * Unlock the other which
   should fail

   If `order`, unlock 10 first, verify 20 will not keep thinking 0 is its
   predecessor and discover 10 actually is.

   If `not order`, unlock 20 first and let it find 0 as a predecessor. Check 10
   discovers 100 is not its successor anymore, 20 is. *)

let chord_join_race order () =
  let* dht_pred = Dht.make 0 [] in
  let* dht_succ = Dht.make 100 [ Dht.endpoint dht_pred ] in
  let eps = [ Dht.endpoint dht_pred; Dht.endpoint dht_succ ] in
  let filter = function
    | { Transport.filtered } -> (
      function
      | ChordMessage.Hello _ -> filtered = 0
      | _ -> false )
  in
  let transport_a = Transport.make_details filter
  and transport_b = Transport.make_details filter in
  let make_dht transport addr pred =
    let* dht = Dht.make_details addr transport eps in
    let+ predecessor = Dht.predecessor dht in
    let () = Alcotest.(check address "predecessor" pred predecessor) in
    dht
  in
  let dht_a = make_dht transport_a 10 0
  and dht_b =
    make_dht transport_b 20
      ( if order then
        10
      else
        0 )
  in
  let* qa = Lwt.map ~f:Result.return (Rpc.receive transport_a.query)
  and* qb = Lwt.map ~f:Result.return (Rpc.receive transport_b.query) in
  let check_query s p = function
    | ChordMessage.Hello { self; predecessor } ->
      Alcotest.(check address "self" s self.address);
      Alcotest.(check address "self" p predecessor)
    | _ -> Alcotest.fail "wrong query"
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
  let* () =
    let+ predecessor = Dht.predecessor dht_a in
    Alcotest.(check address "predecessor A" exp_a predecessor)
  in
  let* () =
    let+ predecessor = Dht.predecessor dht_b in
    Alcotest.(check address "predecessor B" exp_b predecessor)
  in
  Lwt_result.return ()

let chord_fix_index_overshoot () =
  let count = ref 0 in
  let make_details addr =
    let filter _ = function
      | ChordMessage.Successor _ ->
        count := !count + 1;
        false
      | _ -> false
    in
    let transport = Transport.make_details filter in
    Dht.make_details addr transport
  in
  (* Setup so dht_0 wrongfully thinks dht_2 is its successor. *)
  Logs.info (fun m -> m "create DHTs");
  let* dht_0 = make_details 0 [] in
  let* dht_2 = make_details 2 [ Dht.endpoint dht_0 ] in
  let* dht_1 = make_details 1 [ Dht.endpoint dht_2 ] in
  Logs.info (fun m -> m "set key 1");
  let* () = Dht.set dht_1 1 (Bytes.of_string "") in
  let before = !count in
  Logs.info (fun m -> m "get key 1");
  let* _ = Dht.get dht_0 1 in
  (* The first run must fix the finger table. *)
  Alcotest.(check int_less "hops" (before + 1) !count);
  let before = !count in
  let* _ = Dht.get dht_0 1 in
  (* The second run must find the value in one hop. *)
  Alcotest.(check int "hops" (before + 1) !count);
  Lwt_result.return ()

let chord_complexity () =
  let rec enumerate acc = function
    | 0 -> 0 :: acc
    | n -> enumerate (n :: acc) (n - 1)
  in
  let addresses = enumerate [] (((2 ** Address.space_log) - 1) / 10) in
  let count = ref 0 in
  let* endpoints, dhts =
    let module List = List_monadic.Make2 (Lwt_result) in
    List.fold_map addresses ~init:[] ~f:(fun endpoints address ->
        let filter _ = function
          | ChordMessage.Successor _ ->
            count := !count + 1;
            false
          | _ -> false
        in
        let transport = Transport.make_details filter in
        let* dht = Dht.make_details address transport endpoints in
        Lwt_result.return (Dht.endpoint dht :: endpoints, dht))
  in
  ignore endpoints;
  ignore dhts;
  Alcotest.(check int_greater "hops" 50 !count);
  Lwt_result.return ()

let () =
  let open Alcotest_lwt in
  let test_case name f =
    let f _ () =
      let open Let.Syntax (Lwt) in
      let* () = Logs_lwt.info (fun m -> m "start test %s" name) in
      let+ res = f () in
      Alcotest.(check (result unit string) "result" (Result.Ok ()) res)
    in
    test_case name `Quick f
  in
  Lwt_main.run
  @@ run "Doughnut"
       [
         ( "generic",
           [ test_case "single" generic_single; test_case "join" generic_join ]
         );
         ( "chord",
           [
             test_case "join:wrong_predecessor" (chord_join_race true);
             test_case "join:wrong_successor" (chord_join_race false);
             test_case "lookup:index_overshoot" chord_fix_index_overshoot;
             test_case "lookup:complexity" chord_complexity;
           ] );
       ]
