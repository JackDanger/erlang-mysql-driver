%%%-------------------------------------------------------------------
%%% File    : mysql.erl
%%% Author  : Magnus Ahltorp <ahltorp@nada.kth.se>
%%% Descrip.: MySQL client.
%%%
%%% Created :  4 Aug 2005 by Magnus Ahltorp <ahltorp@nada.kth.se>
%%%
%%% Copyright (c) 2001-2004 Kungliga Tekniska Högskolan
%%% See the file COPYING
%%%
%%% Modified : 9/12/2006 by Yariv Sadan <yarivvv@gmail.com>
%%% Note: I added many improvements, including prepared statements,
%%% transactions, better connection pooling, more efficient logging
%%% and other internal enhancements.
%%%
%%% Usage:
%%%
%%%
%%% Call one of the start-functions before any call to fetch/2
%%%
%%%   start_link(PoolId, Host, User, Password, Database)
%%%   start_link(PoolId, Host, Port, User, Password, Database)
%%%   start_link(PoolId, Host, User, Password, Database, LogFun)
%%%   start_link(PoolId, Host, Port, User, Password, Database, LogFun)
%%%
%%% PoolId is a connection pool identifier. If you want to have more
%%% than one connection to a server (or a set of MySQL replicas),
%%% add more with
%%%
%%%   connect(PoolId, Host, Port, User, Password, Database, Reconnect)
%%%
%%% use 'undefined' as Port to get default MySQL port number (3306).
%%% MySQL querys will be sent in a per-PoolId round-robin fashion.
%%% Set Reconnect to 'true' if you want the dispatcher to try and
%%% open a new connection, should this one die.
%%%
%%% When you have a mysql_dispatcher running, this is how you make a
%%% query :
%%%
%%%   fetch(PoolId, "select * from hello") -> Result
%%%     Result = {data, MySQLRes} | {updated, MySQLRes} |
%%%              {error, MySQLRes}
%%%
%%% Actual data can be extracted from MySQLRes by calling the following API
%%% functions:
%%%     - on data received:
%%%          FieldInfo = mysql:get_result_field_info(MysqlRes)
%%%          AllRows   = mysql:get_result_rows(MysqlRes)
%%%         with FieldInfo = list() of {Table, Field, Length, Name}
%%%          and AllRows   = list() of list() representing records
%%%     - on update:
%%%          Affected  = mysql:get_result_affected_rows(MysqlRes)
%%%         with Affected  = integer()
%%%     - on error:
%%%          Reason    = mysql:get_result_reason(MysqlRes)
%%%         with Reason    = string()
%%% 
%%% If you just want a single MySQL connection, or want to manage your
%%% connections yourself, you can use the mysql_conn module as a
%%% stand-alone single MySQL connection. See the comment at the top of
%%% mysql_conn.erl.
%%%
%%%-------------------------------------------------------------------
-module(mysql).
-behaviour(gen_server).

%%--------------------------------------------------------------------
%% External exports
%%--------------------------------------------------------------------
-export([start_link/5,
	 start_link/6,
	 start_link/7,

	 fetch/2,
	 fetch/3,

	 prepare/2,
	 execute/2,
	 execute/3,
	 execute/4,
	 unprepare/1,

	 new_transaction/1,
	 add_query/2,
	 add_queries/2,
	 add_execute/2,
	 add_execute/3,
	 add_executes/2,
	 commit/1,
	 commit/2,
	 transaction/2,
	 transaction/3,

	 get_result_field_info/1,
	 get_result_rows/1,
	 get_result_affected_rows/1,
	 get_result_reason/1,

	 encode/1,
	 encode/2,
	 quote/1,
	 asciz_binary/2,

	 connect/7
	]).

%%--------------------------------------------------------------------
%% Internal exports - just for mysql_* modules
%%--------------------------------------------------------------------
-export([log/4
	]).

%%--------------------------------------------------------------------
%% Internal exports - gen_server callbacks
%%--------------------------------------------------------------------
-export([init/1,
	 handle_call/3,
	 handle_cast/2,
	 handle_info/2,
	 terminate/2,
	 code_change/3
	]).

%%--------------------------------------------------------------------
%% Records
%%--------------------------------------------------------------------
-include("mysql.hrl").

-record(conn, {
	  pool_id,           %% atom(), the pool's Id
	  pid,          %% pid(), mysql_conn process	 
	  reconnect,	%% true | false, should mysql_dispatcher try
                        %% to reconnect if this connection dies?
	  host,		%% string()
	  port,		%% integer()
	  user,		%% string()
	  password,	%% string()
	  database,	%% string()

	  prepares = gb_sets:empty() %% a set of StmtName atoms() indicating
	      %% which statements were prepared for this connection
	 }).


-record(state, {
	  conn_pools = gb_trees:empty(), %% gb_tree mapping connection
	                %% pool id to a connection pool tuple

	  pids_pools = gb_trees:empty(), %% gb_tree mapping connection Pid
	                                 %% to pool id

	  log_fun,	%% function for logging,

	  prepares = gb_trees:empty() %% maps StmtName (atom()) to binary()
	 }).

-record(transaction,
	{pool_id,
	 queries = [<<"BEGIN">>],
	 prepares = gb_sets:empty()
	}).

%%--------------------------------------------------------------------
%% Macros
%%--------------------------------------------------------------------
-define(SERVER, mysql_dispatcher).
-define(CONNECT_TIMEOUT, 5000).
-define(LOCAL_FILES, 128).
-define(PORT, 3306).

%% used for debugging
-define(L(Obj), io:format("LOG ~w ~p\n", [?LINE, Obj])).


%% Log messages are designed to instantiated lazily only if the logging level
%% permits a log message to be logged
-define(Log(LogFun,Level,Msg),
	LogFun(?MODULE,?LINE,Level,fun()-> {Msg,[]} end)).
-define(Log2(LogFun,Level,Msg,Params),
	LogFun(?MODULE,?LINE,Level,fun()-> {Msg,Params} end)).
			     

log(Module, Line, _Level, FormatFun) ->
    {Format, Arguments} = FormatFun(),
    io:format("~w:~b: "++ Format ++ "~n", [Module, Line] ++ Arguments).


%%====================================================================
%% External functions
%%====================================================================

%%--------------------------------------------------------------------
%% Function: start_link(PoolId, Host, User, Password, Database)
%%           start_link(PoolId, Host, Port, User, Password, Database)
%%           start_link(PoolId, Host, User, Password, Database, LogFun)
%%           start_link(PoolId, Host, Port, User, Password, Database,
%%                      LogFun)
%%           PoolId       = term(), first connection pool id
%%           Host     = string()
%%           Port     = integer()
%%           User     = string()
%%           Password = string()
%%           Database = string()
%%           LogFun   = undefined | function() of arity 3
%% Descrip.: Starts the MySQL client gen_server process.
%% Returns : {ok, Pid} | ignore | {error, Error}
%%--------------------------------------------------------------------
start_link(PoolId, Host, User, Password, Database) ->
    start_link(PoolId, Host, ?PORT, User, Password, Database, undefined).

start_link(PoolId, Host, Port, User, Password, Database) ->
    start_link(PoolId, Host, Port, User, Password, Database, undefined).

start_link(PoolId, Host, undefined, User, Password, Database, LogFun) ->
    start_link(PoolId, Host, ?PORT, User, Password, Database, LogFun);

start_link(PoolId, Host, Port, User, Password, Database, LogFun) ->
    crypto:start(),
    gen_server:start_link(
      {local, ?SERVER}, ?MODULE,
      [PoolId, Host, Port, User, Password, Database, LogFun], []).

%%--------------------------------------------------------------------
%% Function: fetch(PoolId, Query)
%%           fetch(PoolId, Query, Timeout)
%%           PoolId      = term(), connection pool Id
%%           Query   = string(), MySQL query in verbatim
%%           Timeout = integer() | infinity, gen_server timeout value
%% Descrip.: Send a query and wait for the result.
%% Returns : {data, MySQLRes}    |
%%           {updated, MySQLRes} | 
%%           {error, MySQLRes}
%%           MySQLRes = term()
%%--------------------------------------------------------------------
fetch(PoolId, Query) ->
    gen_server:call(?SERVER, {fetch, PoolId, Query}).
fetch(PoolId, Query, Timeout) -> 
   gen_server:call(?SERVER, {fetch, PoolId, Query}, Timeout).

prepare(Name, Query) ->
    gen_server:cast(?SERVER, {prepare, Name, Query}).

unprepare(Name) ->
    gen_server:cast(?SERVER, {unprepare, Name}).

execute(PoolId, Name) ->
    execute(PoolId, Name, []).

execute(PoolId, Name, Timeout) when is_integer(Timeout) ->
    execute(PoolId, Name, [], Timeout);

execute(PoolId, Name, Params) ->
    gen_server:call(?SERVER, {execute, PoolId, Name, Params}).

execute(PoolId, Name, Params, Timeout) ->
    gen_server:call(?SERVER, {execute, PoolId, Name, Params}, Timeout).


%%-------------------------------------------------------------------
%% Function: new_transaction(PoolId)
%%           PoolId = atom() the connection pool id
%% Descrip.: Create a transaction for a previously-created connection
%%           identified by the PoolId parameter.
%% Returns:  a new transaction record
%%-----------------------------------------------------------------
new_transaction(PoolId) ->
    #transaction{pool_id = PoolId}.

%%-------------------------------------------------------------------
%% Function: add_query(Transaction, Query)
%%           Transaction = transaction()
%%           Query = binary() | string()  the SQL query
%% Descrip.: Add a query to the transaction record, which was started
%%           with start_transaction/1
%% Returns:  ok
%%-----------------------------------------------------------------

add_query(Transaction, Query) when is_list(Query) ->
    add_query(Transaction, list_to_binary(Query));

add_query(#transaction{queries = Queries} = Transaction, Query) ->
    Transaction#transaction{queries = [Query|Queries]}.

%% Performs add_query on a list of queries
add_queries(Transaction, QueryList) ->
    lists:foldl(
      fun(Query, T1) ->
	      add_query(T1, Query)
      end, Transaction, QueryList).


%%-------------------------------------------------------------------
%% Function: add_execute(Transaction, StmtName, Params)
%%           Transaction = transaction()
%%           StmtName = atom() the name of a prepared statement previosly
%%           created with prepare/1
%%           Params = [term()] (optional) the list of parameters for the
%%           prepared statement. The size of the list must match the
%%           arity of the prepared statement, or MySQL will report an error.
%%           If the statement takes no parameters, you can call
%%           add_execute/2.
%% Descrip.: add a prepared statement execution to the transaction
%% Returns:  the modified transaction record
%%-----------------------------------------------------------------

add_execute(Transaction, StmtName) ->
    add_execute(Transaction, StmtName, []).

add_execute(#transaction{queries = Queries,
			 prepares = Prepares} = Transaction,
	    StmtName, Params) ->
    Stmts = make_statements_for_execute(StmtName, Params),
    Transaction#transaction{queries = lists:reverse(Stmts) ++ Queries,
			    prepares = gb_sets:add(StmtName, Prepares)}.

%% Performs add_execute on a list of StmtName atoms and/or
%% {StmtName, Params} tuples.
add_executes(Transaction, Executes) ->
    lists:foldl(
      fun({StmtName, Params}, T1) ->
	      add_execute(T1, StmtName, Params);
	 (StmtName, T1) ->
	      add_execute(T1, StmtName)
      end, Transaction, Executes).

%%-------------------------------------------------------------------
%% Function: commit(Transaction)
%%           transaction = transaction()
%% Descrip.: Commit the current transaction.
%% Returns:  {updated, MySQLRes} | {error, MySQLRes}
%%           MySQLRes = term()
%%-----------------------------------------------------------------
commit(Transaction) ->
    gen_server:call(?SERVER, {commit, Transaction}).

commit(Transaction, Timeout) ->
    gen_server:call(?SERVER, {commit, Transaction}, Timeout).

%%-------------------------------------------------------------------
%% Function: transaction(PoolId, Fun)
%%           Id = atom() the connection pool id
%%           Fun = function(Transaction) -> NewTransaction
%%             a function containing one or more
%%             calls to add_query/2 or add_execute/3.
%% Descrip.: Execute the function Fun in a transaction context.
%% Returns:  {updated, MySQLRes} | {error, MySQLRes}
%%           MySQLRes = term()
%%-----------------------------------------------------------------
transaction(PoolId, Fun) ->
    Transaction = new_transaction(PoolId),
    NewTransaction = Fun(Transaction),
    commit(NewTransaction).

transaction(PoolId, Fun, Timeout) ->
    Transaction = new_transaction(PoolId),
    NewTransaction = Fun(Transaction),
    commit(NewTransaction, Timeout).

%%--------------------------------------------------------------------
%% Function: get_result_field_info(MySQLRes)
%%           MySQLRes = term(), result of fetch function on "data"
%% Descrip.: Extract the FieldInfo from MySQL Result on data received
%% Returns : FieldInfo
%%           FieldInfo = list() of {Table, Field, Length, Name}
%%--------------------------------------------------------------------
get_result_field_info(#mysql_result{fieldinfo = FieldInfo}) ->
    FieldInfo.

%%--------------------------------------------------------------------
%% Function: get_result_rows(MySQLRes)
%%           MySQLRes = term(), result of fetch function on "data"
%% Descrip.: Extract the Rows from MySQL Result on data received
%% Returns : Rows
%%           Rows = list() of list() representing records
%%--------------------------------------------------------------------
get_result_rows(#mysql_result{rows=AllRows}) ->
    AllRows.

%%--------------------------------------------------------------------
%% Function: get_result_affected_rows(MySQLRes)
%%           MySQLRes = term(), result of fetch function on "updated"
%% Descrip.: Extract the Rows from MySQL Result on update
%% Returns : AffectedRows
%%           AffectedRows = integer()
%%--------------------------------------------------------------------
get_result_affected_rows(#mysql_result{affectedrows=AffectedRows}) ->
    AffectedRows.

%%--------------------------------------------------------------------
%% Function: get_result_reason(MySQLRes)
%%           MySQLRes = term(), result of fetch function on "error"
%% Descrip.: Extract the error Reason from MySQL Result on error
%% Returns : Reason
%%           Reason    = string()
%%--------------------------------------------------------------------
get_result_reason(#mysql_result{error=Reason}) ->
    Reason.

%%--------------------------------------------------------------------
%% Function: connect(PoolId, Host, Port, User, Password, Database,
%%                   Reconnect)
%%           PoolId        = term(), connection-group Id
%%           Host      = string()
%%           Port      = undefined | integer()
%%           User      = string()
%%           Password  = string()
%%           Database  = string()
%%           Reconnect = true | false
%% Descrip.: Starts a MySQL connection and, if successfull, registers
%%           it with the mysql_dispatcher.
%% Returns : {ok, ConnPid} | {error, Reason}
%%--------------------------------------------------------------------
connect(PoolId, Host, undefined, User, Password, Database, Reconnect) ->
    connect(PoolId, Host, ?PORT, User, Password, Database, Reconnect);
connect(PoolId, Host, Port, User, Password, Database, Reconnect) ->
   {ok, LogFun} = gen_server:call(?SERVER, get_logfun),
    case mysql_conn:start(Host, Port, User, Password, Database, LogFun) of
	{ok, ConnPid} ->
	    Conn = new_conn(PoolId, ConnPid, Reconnect, Host, Port, User,
				 Password, Database),
	    case gen_server:call(
		   ?SERVER, {add_conn, Conn}) of
		ok ->
		    {ok, ConnPid};
		Res ->
		    Res
	    end;
	{error, Reason} ->
	    {error, Reason}
    end.

new_conn(PoolId, ConnPid, Reconnect, Host, Port, User, Password, Database) ->
    case Reconnect of
	true ->
	    #conn{pool_id = PoolId,
		  pid = ConnPid,
		  reconnect = true,
		  host = Host,
		  port = Port,
		  user = User,
		  password = Password,
		  database = Database
		 };
	false ->                        
	    #conn{pool_id = PoolId,
		  pid = ConnPid,
		  reconnect = false}
    end.


%%====================================================================
%% gen_server callbacks
%%====================================================================

init([PoolId, Host, Port, User, Password, Database, LogFun]) ->
    LogFun1 = if LogFun == undefined -> fun log/4; true -> LogFun end,
    case mysql_conn:start(Host, Port, User, Password, Database, LogFun1) of
	{ok, ConnPid} ->
	    Conn = new_conn(PoolId, ConnPid, true, Host, Port, User, Password,
				 Database),
	    State = #state{log_fun = LogFun1},
	    {ok, add_conn(Conn, State)};
	{error, Reason} ->
	    ?Log(LogFun1, error,
		 "failed starting first MySQL connection handler, "
		 "exiting"),
	    {stop, {error, Reason}}
    end.

handle_call({fetch, PoolId, Query}, From, State) ->
    fetch_queries(PoolId, From, State, [Query], false);

handle_call({execute, PoolId, StmtName, Params}, From, State) ->
    Stmts = make_statements_for_execute(StmtName, Params),
    fetch_queries(PoolId, From, State, Stmts, [StmtName], false);

handle_call({add_conn, Conn}, _From, State) ->
    NewState = add_conn(Conn, State),
    {PoolId, ConnPid} = {Conn#conn.pool_id, Conn#conn.pid},
    LogFun = State#state.log_fun,
    ?Log2(LogFun, normal,
	  "added connection with id '~p' (pid ~p) to my list",
	  [PoolId, ConnPid]),
    {reply, ok, NewState};

handle_call(get_logfun, _From, State) ->
    {reply, {ok, State#state.log_fun}, State};

handle_call({commit, #transaction{pool_id = PoolId,
				 queries = Queries,
				 prepares = Prepares}},
	     From, State) ->
    fetch_queries(PoolId, From, State,
		  lists:reverse([<<"COMMIT">> | Queries]),
		  gb_sets:to_list(Prepares), true).

fetch_queries(PoolId, From, State, QueryList, RollbackOnError) ->
    fetch_queries(PoolId, From, State, QueryList, undefined, RollbackOnError).

fetch_queries(PoolId, From, State, QueryList, Prepares, RollbackOnError) ->
    case get_next_conn(PoolId, State) of
	{ok, Conn, NewState} ->
	    Pid = Conn#conn.pid,
	    ConnPrepares = Conn#conn.prepares,
	    case make_prepare_queries(State, QueryList, ConnPrepares,
				      Prepares) of
		{error, Err} ->
		    {reply, Err, NewState};
		{ok, {QueryList2, ConnPrepares2}} -> 
		    mysql_conn:fetch_queries(Pid, QueryList2, From,
					     RollbackOnError),

		    %% TODO what if an error occurs on a prepared
		    %% statement???
		    Cond = gb_sets:size(ConnPrepares) ==
			gb_sets:size(ConnPrepares2),
		    NewState1 =
			if (Cond) ->
				NewState;
			   true ->
				Conn1 = Conn#conn{prepares=ConnPrepares2},
				replace_conn(Conn1, NewState)
			end,

		    %% The ConnPid process does a gen_server:reply() when
		    %% it has an answer
		    {noreply, NewState1}
	    end;
	nomatch ->
	    %% we have no active connection matching PoolId
	    {reply, {error, no_connection}, State}
    end.

%% Get a modified list of queries and set of prepared statements for
%% the connection based on the set of prepared statements to be
%% executed in this transaction. If the connection already has
%% prepared all the statements, the original lists are returned.
%% If the transaction contains a call to an undefined prepared statement,
%% an error is returned.
%%
%% returns {NewQueryList, NewConnPrepares} or {error, Error}
make_prepare_queries(State, QueryList, ConnPrepares, Prepares) ->
    if Prepares == undefined ->
	    {ok, {QueryList, ConnPrepares}};
       true ->
	    catch
		lists:foldl(
		  fun(StmtName, {ok, {QueryList1, ConnPrepares1}}) ->
			  case gb_sets:is_element(StmtName, ConnPrepares) of
			      true ->
				  {ok, {QueryList1, ConnPrepares1}};
			      false ->
				  case find_statement(State, StmtName) of
				      {ok, StmtBin} ->
					  {ok, {[StmtBin | QueryList1],
					   gb_sets:add_element(
					     StmtName, ConnPrepares1)}};
				      error ->
					  throw({error,
						 {unrecognized_statement,
						  StmtName}})
				  end
			  end
		  end, {ok, {QueryList, ConnPrepares}}, Prepares)
	    end.

make_statements_for_execute(StmtName, []) ->
    StmtNameBin = encode(StmtName, true),
    [<<"EXECUTE ", StmtNameBin/binary>>];
make_statements_for_execute(StmtName, Params) ->
    NumParams = length(Params),
    ParamNums = lists:seq(1, NumParams),

    StmtNameBin = encode(StmtName, true),
    ParamNames =
	lists:foldl(
	  fun(Num, Acc) ->
		  Name = "@" ++ integer_to_list(Num),
		  if Num == 1 ->
			  Name ++ Acc;
		     true ->
			  [$, | Name] ++ Acc
		  end
	  end, [], lists:reverse(ParamNums)),
    ParamNamesBin = list_to_binary(ParamNames),

    ExecStmt = <<"EXECUTE ", StmtNameBin/binary, " USING ",
		ParamNamesBin/binary>>,

    ParamVals = lists:zip(ParamNums, Params),
    Stmts = lists:foldl(
	      fun({Num, Val}, Acc) ->
		      NumBin = encode(Num, true),
		      ValBin = encode(Val, true),
		      [<<"SET @", NumBin/binary, "=", ValBin/binary>> | Acc]
	       end, [ExecStmt], ParamVals),
    Stmts.

handle_cast({prepare, StmtName, Statement}, State) ->
    LogFun = State#state.log_fun,
    ?Log2(LogFun, debug,
	"received prepare/2: ~p ~p", [StmtName, Statement]),
    StmtBin = if is_list(Statement) -> list_to_binary(Statement);
		 true -> Statement
	      end,
    StmtNameBin = list_to_binary(atom_to_list(StmtName)),
    StmtBin1 = <<"PREPARE ", StmtNameBin/binary, " FROM '",
		StmtBin/binary, "'">>,
    State1 = remove_prepare(State, StmtName, false),
    {noreply, State1#state{prepares =
			  gb_trees:enter(StmtName, StmtBin1,
				     State1#state.prepares)}};

handle_cast({unprepare, StmtName}, State) ->
    LogFun = State#state.log_fun,
    ?Log2(LogFun, debug, "received unprepare/1: ~p", [StmtName]),

    {noreply, remove_prepare(State, StmtName, true)}.

remove_prepare(State, StmtName, WarnIfMissing) ->
    Prepares = State#state.prepares,
    case catch gb_trees:delete(StmtName, Prepares) of
	{'EXIT', _} ->
	    if WarnIfMissing ->
		    LogFun = State#state.log_fun,
		    ?Log2(LogFun, warn, "tried to remove a non-existing "
			  "statement ~p", [StmtName]);
	       true ->
		    ok
	    end,
	    State;
	NewPrepares ->
	    RemoveFun =
		fun(Conn) ->
			Conn#conn{prepares =
				  gb_sets:delete_any(StmtName,
						     Conn#conn.prepares)}
		end,

	    NewConnPools = 
		lists:foldl(
		  fun({PoolId, {Used, Unused}}, NewPools) ->
			  gb_trees:enter(
			    PoolId,
			    {lists:map(RemoveFun,Used),
			     lists:map(RemoveFun,Unused)},
			    NewPools)
		  end,
		  gb_trees:empty(),
		  gb_trees:to_list(State#state.conn_pools)),
	    State#state{
	      prepares = NewPrepares,
	      conn_pools = NewConnPools}
    end.
			  
find_statement(State, StmtName) ->
    case gb_trees:lookup(StmtName, State#state.prepares) of
	none ->
	    LogFun = State#state.log_fun,
	    ?Log2(LogFun, error,
		  "received execute command for an unrecognized "
		  "statement: ~p", [StmtName]),
	    error;
	{value, Val} ->
	    {ok, Val}
    end.

%%--------------------------------------------------------------------
%% Function: handle_info({'DOWN', ...}, State)
%% Descrip.: Handle a message that one of our monitored processes
%%           (mysql_conn processes in our connection list) has exited.
%%           Remove the entry from our list.
%% Returns : {noreply, NewState}   |
%%           {stop, normal, State}
%%           NewState = state record()
%%
%% Note    : For now, we stop if our connection list becomes empty.
%%           We should try to reconnect for a while first, to not
%%           eventually stop the whole OTP application if the MySQL-
%%           server is shut down and the mysql_dispatcher was super-
%%           vised by an OTP supervisor.
%%--------------------------------------------------------------------
handle_info({'DOWN', _MonitorRef, process, Pid, Info}, State) ->
    LogFun = State#state.log_fun,
    case remove_conn(Pid, State) of
	{ok, Conn, NewState} ->
	    LogLevel = case Info of
			   normal -> normal;
			   _ -> error
		       end,
	    ?Log2(LogFun, LogLevel,
		"connection pid ~p exited : ~p", [Pid, Info]),
	    case Conn#conn.reconnect of
		true ->
		    start_reconnect(Conn, LogFun);
		false ->
		    ok
	    end,
	    {noreply, NewState};

	error ->
	    ?Log2(LogFun, error,
		  "received 'DOWN' signal from pid ~p not in my list", [Pid]),
	    {noreply, State}
    end.
    
terminate(Reason, State) ->
    LogFun = State#state.log_fun,
    LogLevel = case Reason of
		   normal -> debug;
		   _ -> error
	       end,
    ?Log2(LogFun, LogLevel, "terminating with reason: ~p", [Reason]),
    Reason.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================

add_conn(Conn, State) ->
    Pid = Conn#conn.pid,
    erlang:monitor(process, Conn#conn.pid),
    PoolId = Conn#conn.pool_id,
    ConnPools = State#state.conn_pools,
    NewPool = 
	case gb_trees:lookup(PoolId, ConnPools) of
	    none ->
		{[Conn],[]};
	    {value, {Unused, Used}} ->
		{[Conn | Unused], Used}
	end,
    State#state{conn_pools =
		gb_trees:enter(PoolId, NewPool,
			       ConnPools),
		pids_pools = gb_trees:enter(Pid, PoolId,
					    State#state.pids_pools)}.

replace_conn(Conn, State) ->
    PoolId = Conn#conn.pool_id,
    ConnPools = State#state.conn_pools,
    {value, {Unused, Used}} = gb_trees:lookup(PoolId, ConnPools),
    NewPool =
	case replace_conn_in_list(Conn, Unused) of
	    {Unused1, true} ->
		{Unused1, Used};
	    {_Unused1, false} ->
		{Used1, true} = replace_conn_in_list(Conn, Used),
		{Unused, Used1}
	end,

    State#state{conn_pools = gb_trees:enter(PoolId, NewPool,
					    ConnPools)}.

replace_conn_in_list(Conn, Conns) ->
    lists:mapfoldl(
      fun(Conn1, _Result) when Conn#conn.pid == Conn1#conn.pid ->
	      {Conn, true};
	 (Conn1, Result) ->
	      {Conn1, Result}
      end, false, Conns).

remove_pid_from_list(Pid, Conns) ->
    lists:foldl(
      fun(OtherConn, {NewConns, undefined}) ->
	      if OtherConn#conn.pid == Pid ->
		      {NewConns, OtherConn};
		 true ->
		      {[OtherConn | NewConns], undefined}
	      end;
	 (OtherConn, {NewConns, FoundConn}) ->
	      {[OtherConn|NewConns], FoundConn}
      end, {[],undefined}, lists:reverse(Conns)).
		  

remove_pid_from_lists(Pid, Conns1, Conns2) ->
    case remove_pid_from_list(Pid, Conns1) of
	{NewConns1, undefined} ->
	    {NewConns2, Conn} = remove_pid_from_list(Pid, Conns2),
	    {Conn, {NewConns1, NewConns2}};
	{NewConns1, Conn} ->
	    {Conn, {NewConns1, Conns2}}
    end.
    
remove_conn(Pid, State) ->
    PidsPools = State#state.pids_pools,
    case gb_trees:lookup(Pid, PidsPools) of
	none ->
	    error;
	{value, PoolId} ->
	    ConnPools = State#state.conn_pools,
	    case gb_trees:lookup(PoolId, ConnPools) of
		none ->
		    error;
		{value, {Unused, Used}} ->
		    {Conn, NewPool} = remove_pid_from_lists(Pid, Unused, Used),
		    NewConnPools = gb_trees:enter(PoolId, NewPool, ConnPools),
		    {ok, Conn, State#state{conn_pools = NewConnPools,
				     pids_pools =
				     gb_trees:delete(Pid, PidsPools)}}
	    end
    end.

get_next_conn(PoolId, State) ->
    ConnPools = State#state.conn_pools,
    case gb_trees:lookup(PoolId, ConnPools) of
	none ->
	    error;
	{value, {[],[]}} ->
	    error;
	
	%% We maintain 2 lists: one for unused connections and one for used
	%% connections. When we run out of unused connections, we recycle
	%% the list of used connections. This is more efficient than
	%% using one list and appending the last used connection to the end
	%% of it every time, as the previous algorithm did.
	{value, {[], Used}} ->
	    [Conn | Conns] = lists:reverse(Used),
	    {ok, Conn,
	     State#state{conn_pools =
			 gb_trees:enter(PoolId, {Conns, [Conn]}, ConnPools)}};
	{value, {[Conn|Unused], Used}} ->
	    {ok, Conn, State#state{
			 conn_pools =
			 gb_trees:enter(PoolId, {Unused, [Conn|Used]},
					ConnPools)}}
    end.

start_reconnect(Conn, LogFun) ->
    Pid = spawn(fun () ->
			reconnect_loop(Conn#conn{pid = undefined}, LogFun, 0)
		end),
    {PoolId, Host, Port} = {Conn#conn.pool_id, Conn#conn.host, Conn#conn.port},
    ?Log2(LogFun, debug,
	"started pid ~p to try and reconnect to ~p:~s:~p (replacing "
	"connection with pid ~p)",
	[Pid, PoolId, Host, Port, Conn#conn.pid]),
    ok.

reconnect_loop(Conn, LogFun, N) ->
    {PoolId, Host, Port} = {Conn#conn.pool_id, Conn#conn.host, Conn#conn.port},
    case connect(PoolId,
		 Host,
		 Port,
		 Conn#conn.user,
		 Conn#conn.password,
		 Conn#conn.database,
		 Conn#conn.reconnect) of
	{ok, ConnPid} ->
	    ?Log2(LogFun, debug,
		"managed to reconnect to ~p:~s:~p "
		"(connection pid ~p)", [PoolId, Host, Port, ConnPid]),
	    ok;
	{error, Reason} ->
	    %% log every once in a while
	    NewN = case N of
		       10 ->
			   ?Log2(LogFun, debug,
			       "reconnect: still unable to connect to "
			       "~p:~s:~p (~p)", [PoolId, Host, Port, Reason]),
			   0;
		       _ ->
			   N + 1
		   end,
	    %% sleep between every unsuccessfull attempt
	    timer:sleep(20 * 1000),
	    reconnect_loop(Conn, LogFun, NewN)
    end.


%%--------------------------------------------------------------------
%% Function: encode(Val, AsBinary)
%%           Val = term()
%%           AsBinary = true | false
%% Descrip.: Encode a value so that it can be included safely in a
%%           MySQL query.
%% Returns : Encoded = string() | binary() | {error, Error}
%%--------------------------------------------------------------------
encode(Val) ->
    encode(Val, false).
encode(Val, false) when Val == undefined; Val == null ->
    "null";
encode(Val, true) when Val == undefined; Val == null ->
    <<"null">>;
encode(Val, false) when is_binary(Val) ->
    binary_to_list(quote(Val));
encode(Val, true) when is_binary(Val) ->
    quote(Val);
encode(Val, true) ->
    list_to_binary(encode(Val,false));
encode(Val, false) when is_atom(Val) ->
    atom_to_list(Val);
encode(Val, false) when is_list(Val) ->
    quote(Val);
encode(Val, false) when is_integer(Val) ->
    integer_to_list(Val);
encode(Val, false) when is_float(Val) ->
    [Res] = io_lib:format("~w", [Val]),
    Res;
encode({{Year, Month, Day}, {Hour, Minute, Second}}, false) ->
    Res = io_lib:format("'~B~B~B~B~B~B'", [Year, Month, Day, Hour,
					  Minute, Second]),
    lists:flatten(Res);
encode({Time1, Time2, Time3}, false) ->
    Res = io_lib:format("'~B~B~B'", [Time1, Time2, Time3]),
    lists:flatten(Res);
encode(Val, _AsBinary) ->
    {error, {unrecognized_value, {Val}}}.

%%--------------------------------------------------------------------
%% Function: quote(Val)
%%           Val = string() | binary()
%% Descrip.: Quote a value so that it can be included safely in a
%%           MySQL query. For most purposes, use encode/1 or encode/2
%%           as it is more powerful.
%% Returns : Quoted = string() | binary()
%%--------------------------------------------------------------------
quote(String) when is_list(String) ->
    [39 | lists:reverse([39 | quote(String, [])])];	%% 39 is $'
quote(Bin) when is_binary(Bin) ->
    list_to_binary(quote(binary_to_list(Bin))).

quote([], Acc) ->
    Acc;
quote([0 | Rest], Acc) ->
    quote(Rest, [$0, $\\ | Acc]);
quote([10 | Rest], Acc) ->
    quote(Rest, [$n, $\\ | Acc]);
quote([13 | Rest], Acc) ->
    quote(Rest, [$r, $\\ | Acc]);
quote([$\\ | Rest], Acc) ->
    quote(Rest, [$\\ , $\\ | Acc]);
quote([39 | Rest], Acc) ->		%% 39 is $'
    quote(Rest, [39, $\\ | Acc]);	%% 39 is $'
quote([34 | Rest], Acc) ->		%% 34 is $"
    quote(Rest, [34, $\\ | Acc]);	%% 34 is $"
quote([26 | Rest], Acc) ->
    quote(Rest, [$Z, $\\ | Acc]);
quote([C | Rest], Acc) ->
    quote(Rest, [C | Acc]).


%%--------------------------------------------------------------------
%% Function: asciz_binary(Data, Acc)
%%           Data = binary()
%%           Acc  = list(), input accumulator
%% Descrip.: Find the first zero-byte in Data and add everything
%%           before it to Acc, as a string.
%% Returns : {NewList, Rest}
%%           NewList = list(), Acc plus what we extracted from Data
%%           Rest    = binary(), whatever was left of Data, not
%%                     including the zero-byte
%%--------------------------------------------------------------------
asciz_binary(<<>>, Acc) ->
    {lists:reverse(Acc), <<>>};
asciz_binary(<<0:8, Rest/binary>>, Acc) ->
    {lists:reverse(Acc), Rest};
asciz_binary(<<C:8, Rest/binary>>, Acc) ->
    asciz_binary(Rest, [C | Acc]).

