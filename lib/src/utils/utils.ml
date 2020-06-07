include Base
module Format = Caml.Format
module Lwt = Utils_lwt
module Lwt_result = Utils_lwt_result
module Result = Utils_result

let lwt_ok = Lwt.map ~f:Result.return

let fail fmt = Format.kasprintf (fun m -> Lwt_result.fail m) fmt

let result_warn default result fmt =
  match result with
  | Result.Ok value ->
    Format.ikfprintf (fun _fmt -> Lwt.return value) Format.str_formatter fmt
  | Result.Error e ->
    let f msg =
      let%lwt () = Log.warn_lwt (fun m -> m "%s: %s" msg e) in
      Lwt.return default
    in
    Format.kasprintf f fmt
