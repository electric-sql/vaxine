-ifndef(VX_PROTO).
-define(VX_PROTO, true).

-type txid() :: term().
-type key() :: binary().
-type bucket() :: binary().

-type antidote_key() :: {key(), bucket()}.
-type antidote_type() :: module().
-type antidote_snapshot_val() :: term().

-type wal_offset() :: term().

-record(vx_cli_start_req,
        {
         opts = [] :: [ {offset, none | wal_offset() | eof} ]
        }).
-record(vx_cli_stop_req, {rep_id = erlang:error(bad_msg) :: reference()}).

-type vx_cli_req_msg() :: #vx_cli_start_req{} | #vx_cli_stop_req{}.

-record(vx_srv_start_res, {rep_id = erlang:error(bad_msg) :: reference()}).
-record(vx_srv_stop_res, {}).

-type vx_srv_res_msg() ::
        #vx_srv_start_res{} | #vx_srv_stop_res{} | {error, term()}.

-record(vx_cli_req,
        { ref = make_ref() :: reference(),
          msg :: vx_cli_req_msg()
        }
       ).

-record(vx_srv_res,
        { ref :: reference(), %% contains client ref
          msg :: vx_srv_res_msg()
        }
       ).

-type tx_ops() :: [ { antidote_key(),
                      antidote_type(),
                      antidote_snapshot_val()
                    }
                  ].

-record(vx_wal_txn,
        { txid :: antidote:txid(),
          dcid :: antidote:dcid(),
          %% Offset for the current transaction.
          wal_offset :: wal_offset(),
          ops :: tx_ops()
        }).

-record(vx_client_msg,
        { pid :: pid(),
          msg :: #vx_wal_txn{},
          %% When await_sync = true, that means tnat
          %% connection will no longer send messages
          %% to the client process, unless it request
          %% more data
          await_sync = false :: boolean()
        }).

-endif.
