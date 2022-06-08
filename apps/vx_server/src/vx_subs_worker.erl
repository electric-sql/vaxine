-module(vx_subs_worker).
-behaviour(ranch_protocol).
-behaviour(gen_server).

-include_lib("vx_server/include/vx_proto.hrl").

-export([ start_link/3,
          init/1,
          handle_call/3,
          handle_cast/2,
          handle_info/2,
          terminate/2
        ]).

-record(state, { socket :: port(),
                 transport :: module(),
                 sub_ids :: [vx_subs_server:sub_id()],
                 server_mon :: reference()
               }).

-spec start_link(ranch:ref(), module(), any()) -> {ok, ConnPid :: pid()}.
start_link(Ref, Transport, ProtoOpts) ->
    Pid = proc_lib:spawn_link(?MODULE, init, [{Ref, Transport, ProtoOpts}]),
    {ok, Pid}.

init({Ref, Transport, _Opts}) ->
    {ok, Socket} = ranch:handshake(Ref),
    ok = Transport:setopts(Socket, [{packet, 4}, {active, once}]),
    vx_subs_server:notify_new_client(self(), Socket),
    gen_server:enter_loop(?MODULE, _Opts,
                          #state{socket = Socket, transport = Transport,
                                 server_mon =
                                     erlang:monitor(process, whereis(vx_subs_server))
                                }).

handle_call(_, _, State) ->
    {reply, {error, not_implemented}, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info({tcp, Socket, Data}, #state{transport = Transport} = State) ->
    try
        ok = Transport:setopts(Socket, [{active, once}]),
        handle_vx_protocol(binary_to_term(Data), State)
    of
        {ok, Req} ->
            handle_request(Req, State);
        {error, Error} ->
            ok = Transport:send(Socket, mk_error(Error)),
            {noreply, State}
    catch
        Type:Error:Stack ->
            logger:error("~p: ~p~n~p~n When handling request ~p~n",
                         [Type, Error, Stack, Data]),
            ok = Transport:send(Socket, mk_error(bad_proto)),
            {stop, {error, bad_proto}}
    end;
handle_info({tcp_error, _, Reason}, #state{socket = Socket, transport = Transport}) ->
    logger:warning("Socket error: ~p", [Reason]),
    Transport:close(Socket),
    {stop, {error, Reason}};
handle_info({tcp_closed, _}, #state{}) ->
    {stop, normal}.

terminate(_, _) ->
    ok.

%%------------------------------------------------------------------------------

handle_request(#vx_sub_subscribe_req{keys = Keys}, #state{sub_ids = SubIds} = State) ->
    {ok, SubId} = vx_subs_server:subscribe(Keys),
    {noreply, State#state{sub_ids = [SubId | SubIds]}};
handle_request(#vx_sub_unsubscribe_req{sub_id = SubId}, #state{sub_ids = SubIds} = State) ->
    ok = vx_subs_server:unsubscribe(SubId),
    {noreply, State#state{sub_ids = lists:delete(SubId, SubIds)}}.


%% FIXME: ignore stable_snapshot together with snapshot for now
handle_vx_protocol(#vx_sub_subscribe_req{} = Req, _) ->
    {ok, Req};
handle_vx_protocol(#vx_sub_unsubscribe_req{sub_id = SubId} = Req, #state{sub_ids = SubIds}) ->
    case lists:member(SubId, SubIds) of
        true ->
            {ok, Req};
        false ->
            {error, bad_subid}
    end;
handle_vx_protocol(_, _) ->
    {error, bad_proto}.


%% FIXME: change to protobuf error message
mk_error(bad_proto) ->
    {error, bad_proto};
mk_error(bad_subid) ->
    {error, bad_subid}.
