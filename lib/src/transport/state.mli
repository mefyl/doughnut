type 'a t

val make : 'a -> 'a t

val run :
  'a t ->
  ('a -> ('a * 'result, 'error) Lwt_result.t) ->
  ('result, 'error) Lwt_result.t
