open Doughnut.Utils
open Cmdliner

open Let.Syntax2 (Lwt_result)

let version = "%%VERSION%%"

let doc = {|DHT toolkit.|}

module Main (Mesh : Doughnut.Mesh.S) = struct
  let address =
    let doc =
      Arg.info ~docs:"DHT" ~docv:"ADDRESS" ~doc:"Address of the node."
        [ "address"; "a" ]
    in
    Arg.(required & opt (some string) None doc)

  let endpoint =
    let parse s =
      Mesh.Transport.Wire.Endpoint.of_string s
      |> Result.map_error ~f:(fun msg -> `Msg msg)
    in
    Arg.conv ~docv:"ENDPOINT" (parse, Mesh.Transport.Wire.Endpoint.pp)

  let peers =
    let doc =
      Arg.info ~docs:"DHT" ~docv:"ENDPOINT" ~doc:"Endpoint of a peer."
        [ "peer"; "p" ]
    in
    Arg.(value & opt_all endpoint [] doc)

  let () =
    Logs.set_level ~all:true (Some Logs.Debug);
    Logs.set_reporter (Logs.format_reporter ());
    Logs_threaded.enable ()

  let main address peers =
    let root =
      let module Mesh = Mesh in
      let open Let.Syntax2 (Lwt_result) in
      let* address = Mesh.Address.of_string address |> Lwt.return in
      let* mesh = Mesh.make address peers in
      let stop_on_signal name () =
        let open Let.Syntax (Lwt) in
        let* () = Logs_lwt.info (fun m -> m "stopping on signal %s" name) in
        Lwt.return @@ Mesh.stop mesh
      in
      let _ =
        let signals = [ (Caml.Sys.sigint, "INT"); (Caml.Sys.sigterm, "TERM") ]
        and set_handler (s, name) =
          Lwt_unix.on_signal s (fun _ -> Lwt.async (stop_on_signal name))
          |> ignore
        in
        List.iter ~f:set_handler signals
      in
      Mesh.wait mesh
    in
    Result.map_error ~f:(fun msg -> `Msg msg) @@ Lwt_main.run root
end

module Mesh =
  Doughnut.Family.Make (Doughnut.Address.Int64) (Doughnut.Transport.Tcp)

let () =
  let module Main = Main (Mesh) in
  let main = Term.term_result Term.(const Main.main $ Main.address $ Main.peers)
  and info = Term.info ~version ~doc "doughnut" in
  Term.exit @@ Term.eval (main, info)
