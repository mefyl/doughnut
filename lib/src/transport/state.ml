open Utils

type 'a action =
  | Action :
      ('a -> ('a * 'result, 'error) Lwt_result.t)
      * ('result, 'error) Result.t Lwt.u
      -> 'a action

type 'a t = {
  actions : 'a action Lwt_stream.t;
  action_push : 'a action option -> unit;
  done_ : ('a, string) Lwt_result.t;
  end_ : ('a, string) Result.t Lwt.u;
}

let make state =
  let open Let.Syntax (Lwt) in
  let done_, end_ = Lwt.wait () in
  let actions, action_push = Lwt_stream.create () in
  let run () =
    let rec loop state =
      try%lwt
        let* (Action (f, resolve)) = Lwt_stream.next actions in
        f state >>= function
        | Result.Ok (state, res) ->
          let () = Lwt.wakeup resolve (Result.Ok res) in
          (loop [@tailcall]) state
        | Result.Error e ->
          let () = Lwt.wakeup resolve (Result.Error e) in
          (loop [@tailcall]) state
      with Lwt_stream.Empty -> Lwt_result.return state
    in
    let%lwt result = loop state in
    let () = Lwt.wakeup end_ result in
    Lwt.return ()
  in
  let () = Lwt.async run in
  { actions; action_push; done_; end_ }

let run { action_push; _ } action =
  let wait, result = Lwt.wait () in
  let () = action_push (Some (Action (action, result))) in
  wait

let stop { action_push; _ } = action_push None

let wait { done_; _ } = done_
