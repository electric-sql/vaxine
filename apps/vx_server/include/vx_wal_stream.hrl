-ifndef(VX_WAL_STREAM_HRL).
-define(VX_WAL_STREAM_HRL, true).

-record(wal_replication_status,
        { key :: {antidote:partition_id(), pid()},
          %% Notification marker, is checked by logging_vnode, and if
          %% it's in the ready state message is send to vx_wal_stream worker.
          %% Worker on the other hand sets this flag to ready each time it
          %% finish processing wal log.
          notification = ready :: sent | ready,
          txdata :: term()
        }
       ).

-endif.
