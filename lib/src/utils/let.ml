module Syntax2 (Monad : Base.Monad.S2) = struct
  let ( >>= ) = Monad.( >>= )

  let ( >>| ) = Monad.( >>| )

  let ( let* ) = ( >>= )

  let ( let+ ) = ( >>| )

  let ( and* ) = Monad.Let_syntax.Let_syntax.both

  let ( and+ ) = Monad.Let_syntax.Let_syntax.both
end

module Syntax (Monad : Base.Monad.S) = struct
  include Syntax2 (struct
    include Monad

    type nonrec ('a, 'b) t = 'a t
  end)
end
