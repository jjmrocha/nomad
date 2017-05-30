%%
%% Copyright 2017 Joaquim Rocha <jrocha@gmailbox.org>
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%

-module(nomad_hlc).

-include("nomad_hlc.hrl").

-record(hlc, {l, c}).
-type timestamp() :: #hlc{}.
-export_type([timestamp/0]).

-define(hlc_timestamp(Logical, Counter), #hlc{l = Logical, c = Counter}).

-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/0, start_link/1, start/0, start/1, stop/1]).
-export([timestamp/0, timestamp/1]).
-export([encode/1, decode/1]).
-export([update/1, update/2]).
-export([compare/2, before/2]).

start_link() ->
	gen_server:start_link(?MODULE, [], []).

start_link(Name) when is_atom(Name) ->
	gen_server:start_link({local, Name}, ?MODULE, [], []).

start() ->
	gen_server:start(?MODULE, [], []).

start(Name) when is_atom(Name) ->
	gen_server:start({local, Name}, ?MODULE, [], []).

stop(Process) ->
	gen_server:stop(Process).

timestamp() -> 
	timestamp(?DEFAULT_CLOCK).

timestamp(Process) -> 
	gen_server:call(Process, {timestamp}).

encode(?hlc_timestamp(Logical, Counter)) ->
	<<Time:64>> = <<Logical:48, Counter:16>>,
	Time.

decode(Time) ->
	<<Logical:48, Counter:16>> = <<Time:64>>,
	?hlc_timestamp(Logical, Counter).

update(ExternalTime) -> 
	update(?DEFAULT_CLOCK, ExternalTime).

update(Process, ExternalTime = ?hlc_timestamp(_, _)) -> 
	gen_server:call(Process, {update, ExternalTime});
update(Process, EncodedExternalTime) ->
	ExternalTime = decode(EncodedExternalTime),
	NewTime = update(Process, ExternalTime),
	encode(NewTime).

compare(?hlc_timestamp(L1, _), ?hlc_timestamp(L2, _)) when L1 < L2 -> -1;
compare(?hlc_timestamp(L, C1), ?hlc_timestamp(L, C2)) when C1 < C2 -> -1;
compare(?hlc_timestamp(L, C), ?hlc_timestamp(L, C)) -> 0;
compare(?hlc_timestamp(_, _), ?hlc_timestamp(_, _)) -> 1;
compare(T1, T2) when T1 < T2 -> -1;
compare(T, T) -> 0;
compare(_, _) -> 1.

before(T1, T2) -> 
	compare(T1, T2) =:= -1.

%% ====================================================================
%% Behavioural functions
%% ====================================================================
-record(state, {last}).

%% init/1
init([]) ->
	error_logger:info_msg("~p starting on [~p]...\n", [?MODULE, self()]),
	{ok, #state{last = current_timestamp()}}.

%% handle_call/3
handle_call({timestamp}, _From, State = #state{last = LastTS}) ->
	Now = wall_clock(),
	Logical = max(Now, LastTS#hlc.l),
	Counter = if
		Logical =:= LastTS#hlc.l -> LastTS#hlc.c + 1;
		true -> 0
	end,
	Timestamp = ?hlc_timestamp(Logical, Counter),
	{reply, Timestamp, State#state{last = Timestamp}};

handle_call({update, ExternalTS}, _From, State = #state{last = LastTS}) ->
	Now = wall_clock(),
	Logical = max(Now, LastTS#hlc.l, ExternalTS#hlc.l),
	Counter = if
		Logical =:= LastTS#hlc.l, LastTS#hlc.l =:= ExternalTS#hlc.l ->
			max(LastTS#hlc.c, ExternalTS#hlc.c) + 1;
		Logical =:= LastTS#hlc.l -> LastTS#hlc.c + 1;
		Logical =:= ExternalTS#hlc.l -> ExternalTS#hlc.c + 1;
		true -> 0
	end,
	Timestamp = ?hlc_timestamp(Logical, Counter),
	{reply, Timestamp, State#state{last = Timestamp}};

handle_call(_Msg, _From, State) ->
	{noreply, State}.

%% handle_cast/2
handle_cast(_Msg, State) ->
	{noreply, State}.

%% handle_info/2
handle_info(_Info, State) ->
	{noreply, State}.

%% terminate/2
terminate(_Reason, _State) ->
	ok.

%% code_change/3
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

wall_clock() -> erlang:convert_time_unit(os:system_time(), native, 1000).

current_timestamp() -> ?hlc_timestamp(wall_clock(), 0).

max(A, B, C) -> max(max(A, B), C).
