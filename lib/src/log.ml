open Utils

let src = Logs.Src.create "doughnut"

let debug_lwt m = Logs_lwt.debug m

let info_lwt m = Logs_lwt.info m

let warn_lwt m = Logs_lwt.warn m

let debug m = debug_lwt m |> Lwt.map ~f:Result.return

let info m = info_lwt m |> Lwt.map ~f:Result.return

let warn m = warn_lwt m |> Lwt.map ~f:Result.return
