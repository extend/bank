%% Copyright (c) 2012-2013, Loïc Hoguin <essen@ninenines.eu>
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

%% @doc API for managing and using database connection pools.
-module(bank).

%% API.
-export([start_pool/3]).
-export([stop_pool/1]).
-export([batch/2]).
-export([execute/3]).
-export([prepare/3]).
-export([query/2]).

%% API.

%% @doc Start a pool of connections to a database.
-spec start_pool(any(), pos_integer(), bank_worker:opts()) -> ok.
start_pool(Ref, NbWorkers, Opts) when NbWorkers > 0 ->
	ok = bank_server:add_pool(Ref),
	{ok, SupPid} = supervisor:start_child(bank_sup,
		{{bank_workers_sup, Ref}, {bank_workers_sup, start_link, []},
			permanent, 5000, supervisor, [bank_workers_sup]}),
	_ = [begin
		{ok, _} = supervisor:start_child(
			SupPid, [Ref, Opts])
	end || _ <- lists:seq(1, NbWorkers)],
	ok.

%% @doc Stop a pool of connections.
-spec stop_pool(any()) -> ok.
stop_pool(Ref) ->
	case supervisor:terminate_child(bank_sup, {bank_workers_sup, Ref}) of
		ok ->
			supervisor:delete_child(bank_sup, {bank_workers_sup, Ref});
		{error, Reason} ->
			{error, Reason}
	end.

%% @doc Perform the batch of operations in the given fun.
-spec batch(any(), bank_worker:batch_fun()) -> any().
batch(Ref, Fun) ->
	WorkerPid = bank_server:get_worker(Ref),
	bank_worker:batch(WorkerPid, Fun).

%% @doc Execute a prepared statement and return the results.
-spec execute(any(), any(), [
		null | true | false | %% Atoms with special meaning.
		atom() | list() | binary() | %% All converted to binary strings.
		integer() |
		float() |
		calendar:date() | calendar:time() | calendar:datetime()])
	-> {ok, non_neg_integer(), non_neg_integer()}
	| {rows, [proplists:proplist()]}.
execute(Ref, Stmt, Params) ->
	WorkerPid = bank_server:get_worker(Ref),
	bank_worker:execute(WorkerPid, Stmt, Params).

%% @doc Create a prepared statement for all connections in the pool.
-spec prepare(any(), any(), string()) -> ok.
prepare(Ref, Stmt, Query) ->
	Workers = bank_server:get_all_workers(Ref),
	_ = [bank_worker:prepare(Pid, Stmt, Query) || Pid <- Workers],
	ok.

%% @doc Execute the given SQL query and return the results.
-spec query(any(), string())
	-> {ok, non_neg_integer(), non_neg_integer()}
	| {rows, [proplists:proplist()]}.
query(Ref, Query) ->
	WorkerPid = bank_server:get_worker(Ref),
	bank_worker:query(WorkerPid, Query).
