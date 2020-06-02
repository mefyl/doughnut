type 'a t

val make : 'a -> 'a t

val run :
  'a t ->
  ('a -> ('a * 'result, 'error) Lwt_result.t) ->
  ('result, 'error) Lwt_result.t

val stop : 'a t -> unit

val wait : 'a t -> ('a, string) Lwt_result.t
