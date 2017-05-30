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

-module(nomad_async).

-behaviour(supervisor).

-define(DEFAULT_JOB_QUEUE, '$nomad.async.default.queue').

%% ====================================================================
%% Behavioural
%% ====================================================================
-export([init/1]).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/0]).
-export([start_queue/1, start_queue/2, stop_queue/1, queue_list/0]).
-export([run/1, run/2, run/3, run/4]).

start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% Starts a new queue
%% Using the options: [{hibernate, 5000}]
-spec start_queue(JobQueue) -> supervisor:startchild_ret()
	when JobQueue :: atom().
start_queue(JobQueue) -> 
	start_queue(JobQueue, [{hibernate, 5000}]).

%% Starts a new queue
%% Options: pool_size - Max number of processes (defaults to erlang:system_info(schedulers)) 
%%          priority - Proccess priority valid values: low, normal, high, max (defaults to normal)
%%          hibernate - Max wait time (in milliseconds) for a message before the process goes to hibernation (defaults to infinity)
-spec start_queue(JobQueue, Options) -> supervisor:startchild_ret()
	when JobQueue :: atom(),
	Options :: [Option],
	Option :: {Key, Value},
	Key :: pool_size | priority | hibernate,
	Value :: term().
start_queue(JobQueue, Options) -> 
	supervisor:start_child(?MODULE, [{local, JobQueue}, Options]).

%% Stops a queue
-spec stop_queue(JobQueue) -> ok | {error, Error}
	when JobQueue :: atom(),
	Error :: term().
stop_queue(JobQueue) -> 
	case whereis(JobQueue) of
		undefined -> ok;
		Pid -> supervisor:terminate_child(?MODULE, Pid)
	end.

%% Returns the list of queues
-spec queue_list() -> list().
queue_list() ->
	ChildList = supervisor:which_children(?MODULE),
	lists:filtermap(fun({_, Pid, _, _}) ->
				case erlang:process_info(Pid, registered_name) of
					{registered_name, Name} -> {true, Name}; 
					_ -> false
				end
		end, ChildList).

%% Submits a function to asynchronous execution
-spec run(Fun) -> ok | {error, Reason}
	when Fun :: fun(() -> any()),
					Reason :: term().
run(Fun) when is_function(Fun, 0) -> 
	run(?DEFAULT_JOB_QUEUE, Fun);
run(_Fun) -> {error, invalid_function}.

%% Submits a function to asynchronous execution
-spec run(JobQueue, Fun) -> ok | {error, Reason}
	when JobQueue :: atom(),
	Fun :: fun(() -> any()),
					Reason :: term().
run(JobQueue, Fun) when is_atom(JobQueue), is_function(Fun, 0) ->
	case find_or_create(JobQueue) of
		{ok, Pid} -> nomad_queue:push(Pid, Fun);
		Other -> Other
	end;
run(_JobQueue, _Fun) -> {error, invalid_function}.

%% Submits a function to asynchronous execution
-spec run(Module, Function, Args) -> ok | {error, Reason}
	when Module :: atom(),
	Function :: atom(),
	Args :: list(),
	Reason :: term().
run(Module, Function, Args) when is_atom(Module), is_atom(Function), is_list(Args) ->
	run(?DEFAULT_JOB_QUEUE, Module, Function, Args);
run(_Module, _Function, _Args) -> {error, invalid_request}.

%% Submits a function to asynchronous execution
-spec run(JobQueue, Module, Function, Args) -> ok | {error, Reason}
	when JobQueue :: atom(), 
	Module :: atom(),
	Function :: atom(),
	Args :: list(),
	Reason :: term().
run(JobQueue, Module, Function, Args) when is_atom(JobQueue), is_atom(Module), is_atom(Function), is_list(Args) ->
	run(JobQueue, fun() -> apply(Module, Function, Args) end);
run(_JobQueue, _Module, _Function, _Args) -> {error, invalid_request}.

%% ====================================================================
%% Supervisor behavioural functions
%% ====================================================================

init([]) ->
	JobQueue = #{id => nomad_queue, 
			start => {nomad_queue, start_link, []}, 
			restart => permanent, 
			type => worker},
	SupFlags = #{strategy => simple_one_for_one, 
			intensity => 2, 
			period => 10},
	{ok, {SupFlags, [JobQueue]}}.

%% ====================================================================
%% Internal functions
%% ====================================================================

find_or_create(JobQueue) ->
	case whereis(JobQueue) of
		undefined -> ?MODULE:start_queue(JobQueue);
		Pid -> {ok, Pid}
	end.