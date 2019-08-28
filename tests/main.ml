type ('a, 'b) vchannel = 'a Event.channel * 'b Event.channel

let new_vchannel () = (Event.new_channel (), Event.new_channel ())

let call (s, r) a =
  Event.wrap (Event.send s a) (fun () -> Event.sync (Event.receive r))

module VThread = struct
  type t =
    { thread: Thread.t
    ; exn: exn option ref
    ; bt: Printexc.raw_backtrace option ref }

  let create f a =
    let exn = ref None and bt = ref None in
    let f a =
      try f a
      with e ->
        exn := Some e ;
        bt := Some (Printexc.get_raw_backtrace ())
    in
    {thread= Thread.create f a; exn; bt}

  let join t =
    Thread.join t.thread ;
    match !(t.exn) with
    | Some exn ->
        Printexc.raise_with_backtrace exn (Core.Option.value_exn !(t.bt))
    | None ->
        ()
end

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
  include Dht.Chord.DirectTransport (Address)

  type filter = Passthrough | Response of response

  type stats = {mutable filtered: int}

  type t =
    { query_filter: stats -> message -> bool
    ; query: (message, filter) vchannel
    ; stats: stats }

  let make_details query_filter =
    {query_filter; query= new_vchannel (); stats= {filtered= 0}}

  let make () = make_details (fun _ _ -> false)

  let send t c m =
    if t.query_filter t.stats m then (
      t.stats.filtered <- t.stats.filtered + 1 ;
      match Event.sync (call t.query m) with
      | Passthrough ->
          send () c m
      | Response r ->
          r )
    else send () c m
end

module Dht = Dht.Chord.MakeDetails (Transport)

let () =
  Logs.set_level (Some Logs.Debug) ;
  Logs.set_reporter (Logs.format_reporter ()) ;
  Logs_threaded.enable ()

open OUnit2

let generic_join _ =
  let addresses = [0; 100; 200; 50; 250; 150] in
  let endpoints, dhts =
    Core.List.fold_map addresses ~init:[] ~f:(fun endpoints address ->
        let dht = Dht.make address endpoints in
        (Dht.endpoint dht :: endpoints, dht))
  in
  ignore endpoints ; ignore dhts

module Thread = VThread

(* Test race conditions when joining the network.

* Create a network with nodes 0 and 100
* Make 10 and 20 join concurrently:
  * Let them both find their neighbors (0 and 100) and block hello messages.
  * Unlock one of them, which completes successfully.
  * Unlock the other which should fail

If `order`, unlock 10 first, verify 20 will not keep thinking 0 is its
predecessor and discover 10 actually is.

If `not order`, unlock 20 first and let it find 0 as a
predecessor. Check 10 discovers 100 is not its successor anymore, 20
is. *)

let chord_join_race order ctxt =
  let dht_pred = Dht.make 0 [] in
  let dht_succ = Dht.make 100 [Dht.endpoint dht_pred] in
  let eps = [Dht.endpoint dht_pred; Dht.endpoint dht_succ] in
  let filter = function
    | {Transport.filtered} -> (
        function Transport.Hello _ -> filtered = 0 | _ -> false )
  in
  let transport_a = Transport.make_details filter
  and transport_b = Transport.make_details filter in
  let thread (transport, addr, pred) =
    let dht = Dht.make_details ~transport addr eps in
    OUnit2.assert_equal ~ctxt ~printer:Address.to_string pred
      (Dht.predecessor dht)
  in
  let thread_a = Thread.create thread (transport_a, 10, 0)
  and thread_b =
    Thread.create thread (transport_b, 20, if order then 10 else 0)
  in
  let _ = Event.sync (Event.receive (fst transport_a.query))
  and _ = Event.sync (Event.receive (fst transport_b.query)) in
  let transport_a, thread_a, transport_b, thread_b =
    if order then (transport_a, thread_a, transport_b, thread_b)
    else (transport_b, thread_b, transport_a, thread_a)
  in
  Event.sync (Event.send (snd transport_a.query) Transport.Passthrough) ;
  Thread.join thread_a ;
  Event.sync (Event.send (snd transport_b.query) Transport.Passthrough) ;
  Thread.join thread_b

let suite =
  "DHT"
  >::: [ "generic" >::: ["join" >:: generic_join]
       ; "chord"
         >::: [ "join"
                >::: [ "wrong_predecessor" >:: chord_join_race true
                     ; "wrong_successor" >:: chord_join_race false ] ] ]

let () = run_test_tt_main suite
