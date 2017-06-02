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

-module(nomad_keeper).

-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/0]).
-export([create/2, drop/1]).

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

create(EtsName, Options) when is_atom(EtsName), is_list(Options) ->
	case gen_server:call(?MODULE, {exists, EtsName}) of
		true ->
			case gen_server:call(?MODULE, {give, EtsName, self()}) of
				{ok, Tab} ->
					flush(Tab, EtsName),
					Tab;
				fail ->
					create(EtsName, Options)
			end;
		false ->
			Server = whereis(?MODULE),
			CleanOptions = lists:keydelete(heir, 1, Options),
			NewOptions = [{heir, Server, EtsName} | CleanOptions],
			ets:new(EtsName, NewOptions)
	end.

drop(Tab) ->
	ets:delete(Tab).

%% ====================================================================
%% Behavioural functions
%% ====================================================================

%% init/1
init([]) ->
	{ok, dict:new()}.

%% handle_call/3
handle_call({exists, EtsName}, _From, Inheritance) ->
	Reply = dict:is_key(EtsName, Inheritance),
	{reply, Reply, Inheritance};

handle_call({give, EtsName, NewOwner}, _From, Inheritance) ->
	case dict:find(EtsName, Inheritance) of
		{ok, Tab} ->
			NewInheritance = dict:erase(EtsName, Inheritance),
			ets:give_away(Tab, NewOwner, EtsName),
			Reply = {ok, Tab},
			{reply, Reply, NewInheritance};
		false ->
			{reply, fail, Inheritance}
	end;
	
handle_call(_Request, _From, Inheritance) ->
	{noreply, Inheritance}.
	
%% handle_cast/2
handle_cast(_Msg, State) ->
	{noreply, State}.

%% handle_info/2
handle_info({'ETS-TRANSFER', Tab, _FromPid, EtsName}, Inheritance) ->
	NewInheritance = dict:store(EtsName, Tab, Inheritance),
	{noreply, NewInheritance};
	
handle_info(_Info, Inheritance) ->
	{noreply, Inheritance};

%% terminate/2
terminate(_Reason, Inheritance) ->
	Tables = dict:to_list(Inheritance),
	drop_tables(Tables).

%% code_change/3
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

flush(Tab, EtsName) ->
	receive
		{'ETS-TRANSFER', Tab, _FromPid, EtsName} -> ok
	after 100 -> ok
	end.

drop_tables([]) -> ok;
drop_tables([{_, Tab}|T]) -> 
	ets:delete(Tab),
	drop_tables(T).
