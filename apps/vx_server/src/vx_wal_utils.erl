-module(vx_wal_utils).
-export([ open_wal/2,
          get_lsn/1,
          get_partition_wal_path/1,
          read_ops_from_log/3
        ]).

%% Extracted from:
%% -include_lib("kernel/src/disk_log.hrl").
-record(continuation,
        {pid = self() :: pid(),
         %% We only know how to handle halt logs here, so silent dialyzer
         pos          :: non_neg_integer(), %% | {integer(), non_neg_integer()},
         b            :: binary() | [] | pos_integer()
        }).
-define(HEADERSZ, 8).

-include_lib("kernel/include/file.hrl").
-include("vx_wal.hrl").

-type file_offset() :: non_neg_integer().
-type log_position() :: {non_neg_integer(), Buffer :: term()}.

-export_type([file_offset/0, log_position/0]).

get_lsn(0) ->
    {0, 0};
get_lsn(eof) ->
    {eof, 0};
get_lsn({LOffset, ROffset} = Offset) when
      is_integer(LOffset),
      is_integer(ROffset),
      LOffset >= 0, ROffset >= LOffset ->
    Offset.

get_partition_wal_path(Partition) ->
    InfoList = [_|_] = disk_log:info(log_path(Partition)),
    LogFile = proplists:get_value(file, InfoList),
    halt    = proplists:get_value(type, InfoList), %% Only handle halt type of the logs
    LogFile.

%% Copied from logging_vnode and simplified for our case
-spec log_path(antidote:partition_id()) -> file:filename().
log_path(Partition) ->
    LogFile = integer_to_list(Partition),
    {ok, DataDir} = application:get_env(antidote, data_dir),
    LogId = LogFile ++ "--" ++ LogFile,
    filename:join(DataDir, LogId).

-spec open_wal(file:filename_all(), {non_neg_integer(), non_neg_integer()}) ->
          {ok, #wal{}}.
open_wal(LogFile, {LWalOffset0, _RWalOffset0} = _Lsn) ->
    {ok, FD, FileCurOffset} = open_log(LogFile, LWalOffset0),
    {ok, #wal{file_pos = FileCurOffset,
                    file_buff = [],
                    file_desc = FD,
                    file_status = more_data,
                    file_name = LogFile
                    }}.

-spec open_log(file:filename_all(), integer() | eof) ->
          {ok, file:fd(), pos_integer() }.
open_log(LogFile, WalPos) ->
    {ok, FD} = file:open(LogFile, [raw, binary, read]),
    case WalPos of
        0 ->
            {ok, _Head} = file:read(FD, ?HEADERSZ),
            {ok, FD, 0};
        eof ->
            {ok, NPos} = file:position(FD, eof),
            {ok, FD, NPos - ?HEADERSZ};
        WalPos when is_integer(WalPos) ->
            case file:read_file_info(FD) of
                {ok, #file_info{size = Size}} when WalPos > Size ->
                    logger:error("invalid wal position is provided: ~p~n", [WalPos]),
                    {error, {invalid_wal_pos, WalPos}};
                {ok, _} ->
                    {ok, NPos} = file:position(FD, WalPos),
                    {ok, FD, NPos};
                {error, Reason} ->
                    {error, Reason}
            end
    end.

-type consume_fun() :: fun(([term()], file_offset(), term()) ->
                                   {ok, term()} | {stop, term()} | {error, term()}).

-spec read_ops_from_log(consume_fun(), term(), #wal{}) -> {ok, term(), #wal{}} |
         {error, term()} | {stop, term(), #wal{}}.
read_ops_from_log(OpsConsumerFun, Acc, #wal{} = Wal) ->
    case read_ops_from_log(Wal#wal.file_desc, Wal#wal.file_name,
                           Wal#wal.file_pos, Wal#wal.file_buff
                          )
    of
        {eof, {FPos1, FBuff1} = _LogPosition} ->
            {ok, Acc, Wal#wal{ file_pos = FPos1,
                               file_buff= FBuff1,
                               file_status = eof
                             }};
        {error, _} = Error ->
            Error;
        {ok, {FPos1, FBuff1}, NewTerms} ->
            %% It's not a mistake, we need previous position here
            TxOffset = generate_pos_in_log(Wal#wal.file_pos, Wal#wal.file_buff),
            Wal1 = Wal#wal{ file_pos = FPos1,
                            file_buff = FBuff1,
                            file_status = more_data
                          },

            case OpsConsumerFun(NewTerms, TxOffset, Acc) of
                {ok, Acc1} ->
                    read_ops_from_log(OpsConsumerFun, Acc1, Wal1);
                {stop, Acc1} ->
                    {stop, Acc1, Wal1};
                {error, _} = Error ->
                    Error
            end
    end.

-spec read_ops_from_log(file:fd(), file:filename_all(), non_neg_integer(), term()) ->
          {error, term()} |
          {ok, log_position(), [term()]} |
          {eof, log_position()}.
read_ops_from_log(Fd, FileName, FPos, FBuffer) ->
    case read_chunk(Fd, FileName, FPos, FBuffer, 1) of
        {eof, LogPosition} ->
            {eof, LogPosition};
        {error, _} = Error ->
            Error;
        {ok, LogPosition, NewTerms}->
            {ok, LogPosition, NewTerms}
    end.

-spec read_chunk(file:fd(), file:filename_all(), non_neg_integer(), term(), non_neg_integer()) ->
          {ok, log_position(), [ {term(), antidote:log_record()} ]} |
          {error, term()} |
          {eof, log_position()}.
read_chunk(Fd, FileName, FPos, FBuff, Amount) ->
    R = disk_log_1:chunk_read_only(Fd, FileName, FPos, FBuff, Amount),
    %% Create terms from the binaries returned from chunk_read_only/5.
    %% 'foo' will do here since Log is not used in read-only mode.
    case disk_log:ichunk_end(R, _Log = foo) of
        {#continuation{pos = FPos1, b = Buffer1}, Terms}
          when FPos == FPos1 ->
            %% The same block but different term position
            {ok, {FPos1, Buffer1}, Terms};
        {#continuation{pos = FPos1, b = Buffer1}, Terms} ->
            {ok, {FPos1, Buffer1}, Terms};
        {#continuation{}, [], _BadBytes} = R ->
            logger:error("fpos: ~p~nbad bytes: ~p~n~p~n", [FPos, _BadBytes, R]),
            {eof, {FPos, FBuff}, []};
%            {error, badbytes};
        {error, _} = Error ->
            Error;
        eof ->
            %% That's ok, just need to keep previous position
            {eof, {FPos, FBuff}, []}
    end.

generate_pos_in_log(FPos, FBuf) ->
    case FBuf of
        [] -> FPos;
        B  ->
            FPos - byte_size(B)
    end.
