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

-module(nomad_sup).

-behaviour(supervisor).

-export([init/1]).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/0]).

start_link() ->
	supervisor:start_link(?MODULE, []).

%% ====================================================================
%% Behavioural functions
%% ====================================================================

%% init/1
init([]) ->
	KEEPER = #{id => nomad_keeper, 
			start => {nomad_keeper, start_link, []}, 
			restart => permanent, 
			type => worker},
	GROUP = #{id => nomad_group, 
			start => {nomad_group, start_link, []}, 
			restart => permanent, 
			type => worker},	
	HLC = #{id => nomad_hlc_sup, 
			start => {nomad_hlc_sup, start_link, []}, 
			restart => permanent, 
			type => supervisor},
	ASYNC = #{id => nomad_async, 
			start => {nomad_async, start_link, []}, 
			restart => permanent, 
			type => supervisor},	
	SupFlags = #{strategy => one_for_one, 
			intensity => 2, 
			period => 10},
	{ok, {SupFlags, [KEEPER, GROUP, HLC, ASYNC]}}.

%% ====================================================================
%% Internal functions
%% ====================================================================
