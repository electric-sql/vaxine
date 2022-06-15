-ifndef(VX_SERVER_HRL).
-define(VX_SERVER_HRL, true).

-type partition() :: integer().
-type sn() :: antidote:snapshot_time().
-type key() :: binary().
-type sub_id() :: reference().

-record(keys_notif, { keys :: [ key() ],
                       snapshot :: sn(),
                       partition :: partition()
                     }).

-record(ping_notif, { snapshot :: sn(),
                      partition :: partition()
                    }).

-type keys_notif() :: #keys_notif{}.
-type ping_notif() :: #ping_notif{}.


-endif.
