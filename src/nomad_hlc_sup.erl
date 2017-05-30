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

-module(nomad_hlc_sup).

-include("nomad_hlc.hrl").

-behaviour(supervisor).

-export([init/1]).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/0]).
-export([start_clock/0, start_clock/1, stop_clock/1, clock_list/0]).

start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_clock() -> 
	supervisor:start_child(?MODULE, []).

start_clock(ClockName) when is_atom(ClockName) -> 
	supervisor:start_child(?MODULE, [ClockName]).

stop_clock(Pid) when is_pid(Pid) -> 
	supervisor:terminate_child(?MODULE, Pid);
stop_clock(ClockName) -> 
	case whereis(ClockName) of
		undefined -> ok;
		Pid -> stop_clock(Pid)
	end.

clock_list() ->
	ChildList = supervisor:which_children(?MODULE),
	lists:filtermap(fun({_, Pid, _, _}) ->
				case erlang:process_info(Pid, registered_name) of
					{registered_name, ?DEFAULT_CLOCK} -> false; 
					{registered_name, Name} -> {true, Name}; 
					_ -> {true, Pid}
				end
		end, ChildList).

%% ====================================================================
%% Behavioural functions
%% ====================================================================

%% init/1
init([]) ->
	Clock = #{id => nomad_hlc, 
			start => {nomad_hlc, start_link, []}, 
			restart => permanent, 
			type => worker},
	SupFlags = #{strategy => simple_one_for_one, 
			intensity => 2, 
			period => 10},
	{ok, {SupFlags, [Clock]}}.

%% ====================================================================
%% Internal functions
%% ====================================================================

