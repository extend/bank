%% Copyright (c) 2012, Loïc Hoguin <essen@ninenines.eu>
%%
%% Permission to use, copy, modify, and/or distribute this software for any
%% purpose with or without fee is hereby granted, provided that the above
%% copyright notice and this permission notice appear in all copies.
%%
%% THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
%% WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
%% MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
%% ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
%% WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
%% ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
%% OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

%% @private
-module(bank_worker).
-behavior(gen_server).

%% API.
-export([start_link/2]).
-export([stop/1]).
-export([batch/2]).
-export([execute/3]).
-export([prepare/3]).
-export([sql_query/2]).

%% gen_server.
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-type client_state() :: any().
-type init_fun() :: fun(() -> {ok, module(), client_state()}).
-type batch_fun_ret() :: {ok, any(), client_state()}.
-type batch_fun() :: fun((module(), client_state()) -> batch_fun_ret()).
-type opts() :: [{init_fun, init_fun()} | {keepalive, non_neg_integer()}].

-export_type([init_fun/0]).
-export_type([batch_fun/0]).
-export_type([opts/0]).

-record(state, {
	driver = undefined :: module(),
	client_state = undefined :: client_state(),
	connect_retry = undefined :: non_neg_integer(),
	keepalive = undefined :: non_neg_integer()
}).

%% API.

%% @doc Start the gen_server.
-spec start_link(any(), opts()) -> {ok, pid()}.
start_link(Ref, Opts) ->
	{ok, WorkerPid} = gen_server:start_link(?MODULE, Opts, []),
	ok = bank_server:add_worker(Ref, WorkerPid),
	{ok, WorkerPid}.

%% @doc Stop the gen_server.
-spec stop(pid()) -> stopped.
stop(ServerPid) ->
	gen_server:call(ServerPid, stop).

%% @doc Run a batch job using this worker.
-spec batch(pid(), batch_fun()) -> batch_fun_ret().
batch(ServerPid, Fun) ->
	gen_server:call(ServerPid, {batch, Fun}, infinity).

%% @doc Execute a prepared statement.
%% @todo Improve list().
-spec execute(pid(), any(), list())
	-> {ok, non_neg_integer(), non_neg_integer()}
	| {rows, [proplists:proplist()]}.
execute(ServerPid, Stmt, Params) ->
	gen_server:call(ServerPid, {execute, Stmt, Params}, infinity).

%% @doc Setup a prepared statement.
-spec prepare(pid(), any(), string()) -> ok.
prepare(ServerPid, Stmt, Query) ->
	gen_server:call(ServerPid, {prepare, Stmt, Query}).

%% @doc Run a single SQL query using this worker.
-spec sql_query(pid(), string())
	-> {ok, non_neg_integer(), non_neg_integer()}
	| {rows, [proplists:proplist()]}.
sql_query(ServerPid, Query) ->
	gen_server:call(ServerPid, {sql_query, Query}, infinity).

%% gen_server.

init(Opts) ->
	{init_fun, InitFun} = lists:keyfind(init_fun, 1, Opts),
	ConnectRetry = proplists:get_value(connect_retry, Opts, 5000),
	Keepalive = proplists:get_value(keepalive, Opts, 10000),
	self() ! {init, InitFun},
	{ok, #state{connect_retry=ConnectRetry, keepalive=Keepalive}}.

handle_call({batch, Fun}, _From, State=#state{
		driver=Driver, client_state=ClientState})
		when Driver =/= undefined ->
	{ok, Result, ClientState2} = Fun(Driver, ClientState),
	{reply, {ok, Result}, State#state{client_state=ClientState2}};
handle_call({execute, Stmt, Params}, _From, State=#state{
		driver=Driver, client_state=ClientState})
		when Driver =/= undefined ->
	handle_results(Driver:execute(Stmt, Params, ClientState), State);
handle_call({prepare, Stmt, Query}, _From, State=#state{
		driver=Driver, client_state=ClientState})
		when Driver =/= undefined ->
	{ok, ClientState2} = Driver:prepare(Stmt, Query, ClientState),
	{reply, ok, State#state{client_state=ClientState2}};
handle_call({sql_query, Query}, _From, State=#state{
		driver=Driver, client_state=ClientState})
		when Driver =/= undefined ->
	handle_results(Driver:sql_query(Query, ClientState), State);
handle_call(stop, _From, State) ->
	{stop, normal, stopped, State};
handle_call(_Request, _From, State) ->
	{reply, ignored, State}.

handle_cast(_Request, State) ->
	{noreply, State}.

%% @todo If bad things happen in InitFun, the socket may be left open.
handle_info({init, InitFun}, State=#state{
		connect_retry=ConnectRetry, keepalive=Keepalive}) ->
	try InitFun() of
		{ok, Driver, ClientState} ->
			_ = erlang:send_after(Keepalive, self(), keepalive),
			{noreply, State#state{driver=Driver, client_state=ClientState}}
	catch _:_ ->
		_ = erlang:send_after(ConnectRetry, self(), {init, InitFun}),
		{noreply, State}
	end;
handle_info(keepalive, State=#state{
		driver=Driver, client_state=ClientState, keepalive=Keepalive}) ->
	{ok, ClientState2} = Driver:ping(ClientState),
	_ = erlang:send_after(Keepalive, self(), keepalive),
	{noreply, State#state{client_state=ClientState2}};
handle_info(_Info, State) ->
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%% Internal.

handle_results({ok, AffectedRows, InsertID, ClientState}, State) ->
	{reply, {ok, AffectedRows, InsertID}, State#state{client_state=ClientState}};
handle_results({result_set, Fields, ClientState},
		State=#state{driver=Driver}) ->
	{rows, Rows, ClientState2} = Driver:fetch_all(ClientState),
	TaggedRows = tag_rows(Fields, Rows),
	{reply, {rows, TaggedRows}, State#state{client_state=ClientState2}}.

tag_rows(Fields, Rows) ->
	FieldNames = [Name || {field, Name, _, _} <- Fields],
	[lists:zip(FieldNames, Row) || Row <- Rows].
