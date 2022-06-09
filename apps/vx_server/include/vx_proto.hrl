-ifndef(VX_PROTO).
-define(VX_PROTO, true).

-record(vx_sub_subscribe_req,
        {keys :: [binary()],
         snapshot :: antidote:snapshot_time(),
         stable_snapshot = false
        }).

-record(vx_sub_unsubscribe_req,
        { sub_id :: reference() }).

-endif.
