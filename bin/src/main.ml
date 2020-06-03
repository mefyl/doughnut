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

  let endpoint_file =
    let doc =
      Arg.info ~docs:"NETWORKING" ~docv:"PATH"
        ~doc:"File to write our endpoint to." [ "endpoint-file"; "f" ]
    in
    Arg.(value & opt_all string [] doc)

  let peer_file =
    let doc =
      Arg.info ~docs:"NETWORKING" ~docv:"PATH"
        ~doc:"File to read peer endpoint from." [ "peer-file"; "P" ]
    in
    Arg.(value & opt_all string [] doc)

  let () =
    Logs.set_level ~all:true (Some Logs.Debug);
    Logs.set_reporter (Logs.format_reporter ());
    Logs_threaded.enable ()

  let unix_error code op fmt =
    Fmt.kstr
      (fun msg -> Fmt.str "%s: %s: %s" msg op (Unix.error_message code))
      fmt

  let main address endpoint_files peer_files peers =
    let root =
      let switched switch =
        let module Mesh = Mesh in
        let open Let.Syntax2 (Lwt_result) in
        let started endpoint =
          let module List = List_monadic.Make2 (Lwt_result) in
          let f path =
            let f file =
              Lwt_io.write file
                (Mesh.Transport.Wire.Endpoint.to_string endpoint)
            in
            try%lwt
              let%lwt () =
                Lwt_io.with_file ~mode:Lwt_io.output ~perm:0o400
                  ~flags:[ Unix.O_EXCL; Unix.O_CREAT; Unix.O_WRONLY ]
                  path f
              in
              let rm () =
                try%lwt Lwt_unix.unlink path
                with Unix.Unix_error (code, op, _) ->
                  Logs_lwt.warn (fun m ->
                      m "%s"
                        (unix_error code op "unable to remove endpoint file"))
              in
              Lwt_result.return @@ Lwt_switch.add_hook (Some switch) rm
            with Unix.Unix_error (code, op, _) ->
              Lwt_result.fail
              @@ unix_error code op "unable to write endpoint file %s" path
          in
          List.iter ~f endpoint_files
        in
        let* peers =
          let module List = List_monadic.Make2 (Lwt_result) in
          let* file_peers =
            let f path =
              let f file =
                let%lwt str = Lwt_io.read file in
                Lwt.return @@ Mesh.Transport.Wire.Endpoint.of_string str
              in
              try%lwt Lwt_io.with_file ~mode:Lwt_io.input path f
              with Unix.Unix_error (code, op, _) ->
                Lwt_result.fail
                @@ unix_error code op "unable to read endpoint file %s" path
            in
            List.map ~f peer_files
          in
          Lwt_result.return @@ List.concat [ peers; file_peers ]
        in
        let* address = Mesh.Address.of_string address |> Lwt.return in
        let* mesh = Mesh.make ~started address peers in
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
      Lwt_switch.with_switch switched
    in
    Result.map_error ~f:(fun msg -> `Msg msg) @@ Lwt_main.run root
end

module Mesh =
  Doughnut.Family.Make (Doughnut.Address.Int64) (Doughnut.Transport.Tcp)

let () =
  let module Main = Main (Mesh) in
  let main =
    Term.term_result
      Term.(
        const Main.main $ Main.address $ Main.endpoint_file $ Main.peer_file
        $ Main.peers)
  and info = Term.info ~version ~doc "doughnut" in
  Term.exit @@ Term.eval (main, info)
