open Utils

type 'a action =
  | Action :
      ('a -> ('a * 'result, 'error) Lwt_result.t)
      * ('result, 'error) Result.t Lwt.u
      -> 'a action

type 'a t = {
  actions : 'a action Lwt_stream.t;
  action_push : 'a action option -> unit;
}

let make state =
  let open Let.Syntax (Lwt) in
  let actions, action_push = Lwt_stream.create () in
  let run () =
    let rec loop state =
      let* (Action (f, resolve)) = Lwt_stream.next actions in
      f state >>= function
      | Result.Ok (state, res) ->
        let () = Lwt.wakeup resolve (Result.Ok res) in
        (loop [@tailcall]) state
      | Result.Error e ->
        let () = Lwt.wakeup resolve (Result.Error e) in
        (loop [@tailcall]) state
    in
    loop state
  in
  let () = Lwt.async run in
  { actions; action_push }

let run { action_push; _ } action =
  let wait, result = Lwt.wait () in
  let () = action_push (Some (Action (action, result))) in
  wait
