open Utils

let src = Logs.Src.create "doughnut"

let debug m = Logs_lwt.debug m |> Lwt.map ~f:Result.return

let info m = Logs_lwt.info m |> Lwt.map ~f:Result.return

let warn m = Logs_lwt.warn m |> Lwt.map ~f:Result.return
