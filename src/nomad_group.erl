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

-module(nomad_group).

-define(GROUP_TABLE, '$nomad.group.table').
-define(COUNTER_TABLE, '$nomad.group.counter').

-define(LOCAL_PK(Group), {l, Group}).
-define(REMOTE_PK(Group), {r, Group}).
-define(PID_PK(Pid), {p, Pid}).
-define(GROUP_PK(Group), {g, Group}).

-define(LOCAL_RECORD(Group, Pid), {?LOCAL_PK(Group), Pid}).
-define(REMOTE_RECORD(Group, Node), {?REMOTE_PK(Group), Node}).
-define(PID_RECORD(Pid, Ref, Count), {?PID_PK(Pid), Ref, Count}).
-define(GROUP_RECORD(Group, Count), {?GROUP_PK(Group), Count}).

-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/0]).
-export([join/1, join/2, leave/1, leave/2]).
-export([send/2]).
-export([groups/0, groups/1, members/1, local_members/1]).

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

join(Groups) ->
	join(Groups, self()).

join([], _Pid) -> ok;
join([Group|T], Pid) -> 
	join(Group, Pid),
	join(T, Pid);
join(Group, Pid) ->
	Node = node(Pid),
	gen_server:call({?MODULE, Node}, {join, Group, Pid}).

leave(Groups) ->
	leave(Groups, self()).

leave([], _Pid) -> ok;
leave([Group|T], Pid) -> 
	leave(Group, Pid),
	leave(T,Pid);
leave(Group, Pid) ->
	Node = node(Pid),
	gen_server:call({?MODULE, Node}, {leave, Group, Pid}).

send(Group, Message) ->
	Pids = local_pids(Group),
	send_msg(Pids, Message),
	Nodes = remote_nodes(Group),
	remote_send(Nodes, Group, Message).

groups() ->
	all_groups().

groups(Pid) when node(Pid) =:= node() ->
	local_groups(Pid);
groups(Pid) ->
	Node = node(Pid),
	gen_server:call({?MODULE, Node}, {groups, Pid}).

members(Group) ->
	LocalPids = local_pids(Group),
	Nodes = remote_nodes(Group),
	RemotePids = remote_pids(Nodes, Group),
	LocalPids ++ RemotePids.

local_members(Group) ->
	local_pids(Group).

%% ====================================================================
%% Behavioural functions
%% ====================================================================

%% init/1
init([]) ->
	ok = net_kernel:monitor_nodes(true),
	nomad_keeper:create(?GROUP_TABLE, [bag, protected, named_table, {read_concurrency, true}]),
	nomad_keeper:create(?COUNTER_TABLE, [set, protected, named_table]),
	{ok, Queue} = nomad_queue:start_link([]),
	{ok, Queue, 0}.

%% handle_call/3
handle_call({pids, Group}, From, Queue) ->
	nomad_queue:push(Queue, fun() ->
				Reply = local_pids(Group),
				gen_server:reply(From, Reply)
		end),
	{noreply, Queue};

handle_call({groups, Pid}, From, Queue) ->
	nomad_queue:push(Queue, fun() ->
				Reply = local_groups(Pid),
				gen_server:reply(From, Reply)
		end),
	{noreply, Queue};

handle_call({join, Group, Pid}, _From, State) ->
	join_group(Pid, Group),
	{reply, ok, State};

handle_call({leave, Group, Pid}, _From, State) ->
	leave_group(Pid, Group),
	{reply, ok, State};

handle_call(_Request, _From, State) ->
	{noreply, State}.

%% handle_cast/2
handle_cast({send, Group, Msg, Node}, Queue) ->
	nomad_queue:push(Queue, fun() ->
				case local_pids(Group) of
					[] -> gen_server:cast({?MODULE, Node}, {cancel, Group, node()});
					Pids -> send_msg(Pids, Msg)
				end
		end),
	{noreply, Queue};

handle_cast({forward, Group, Node}, State) ->
	ets:insert(?GROUP_TABLE, ?REMOTE_RECORD(Group, Node)),
	{noreply, State};

handle_cast({cancel, Group, Node}, State) ->
	ets:delete_object(?GROUP_TABLE, ?REMOTE_RECORD(Group, Node)),
	{noreply, State};

handle_cast(_Msg, State) ->
	{noreply, State}.

%% handle_info/2
handle_info({'DOWN', _Ref, process, Pid, _Info}, State) ->
	Groups = local_groups(Pid),
	leave_group(Pid, Groups),
	{noreply, State};

handle_info({nodeup, Node}, State) ->
	Groups = local_groups(),
	lists:foreach(fun(Group) -> 
				gen_server:cast({?MODULE, Node}, {forward, Group, node()})
		end, Groups),
	{noreply, State};

handle_info({nodedown, Node}, State) ->
	ets:select_delete(?GROUP_TABLE, [{?REMOTE_RECORD('_', Node), [], [true]}]),
	{noreply, State};

handle_info(timeout, State) ->
	update_refs(),
	Groups = local_groups(),
	forward_request(Groups),
	{noreply, State};

handle_info(_Info, State) ->
	{noreply, State}.

%% terminate/2
terminate(Reason, _State) ->
	net_kernel:monitor_nodes(false),
	remove_refs(),
	check_drop(Reason),
	ok.

%% code_change/3
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

local_groups() ->
	ets:select(?COUNTER_TABLE, [{?GROUP_RECORD('$1', '_'), [], ['$1']}]).

local_pids(Group) ->
	[ Pid || ?LOCAL_RECORD(_, Pid) <- ets:lookup(?GROUP_TABLE, ?LOCAL_PK(Group)) ].

local_groups(Pid) when is_pid(Pid) ->
	ets:select(?GROUP_TABLE, [{?LOCAL_RECORD('$1', Pid), [], ['$1']}]);
local_groups(Node) ->
	ets:select(?GROUP_TABLE, [{?REMOTE_RECORD('$1', Node), [], ['$1']}]).

remote_nodes(Group) ->
	[ Node || ?REMOTE_RECORD(_, Node) <- ets:lookup(?GROUP_TABLE, ?REMOTE_PK(Group)) ].

update_refs() ->
	Pids = ets:select(?COUNTER_TABLE, [{?PID_RECORD('$1', '_', '_'), [], ['$1']}]),
	lists:foreach(fun(Pid) -> 
				Ref = erlang:monitor(process, Pid),
				ets:update_element(?COUNTER_TABLE, ?PID_PK(Pid), {2, Ref})
		end, Pids).

remove_refs() ->
	Refs = ets:select(?COUNTER_TABLE, [{?PID_RECORD('_', '$1', '_'), [], ['$1']}]),
	lists:foreach(fun(Ref) -> 
				erlang:demonitor(Ref)
		end, Refs).

all_groups() ->
	Groups = local_groups(),
	ets:foldl(fun(?REMOTE_RECORD(Group, _), Acc) -> 
				case lists:member(Group, Acc) of
					false -> [Group|Acc];
					_ -> Acc
				end;
			(_, Acc) -> Acc
		end, Groups, ?GROUP_TABLE).

remote_pids([], _Group) -> [];
remote_pids(Nodes, Group) -> 
	{Replies, _BadNodes} = gen_server:multi_call(Nodes, ?MODULE, {pids, Group}),
	lists:foldl(fun({_, Pids}, Acc) ->
				Pids ++ Acc
		end, [], Replies).

leave_group(_Pid, []) -> ok;
leave_group(Pid, [Group|T]) ->
	leave_group(Pid, Group),
	leave_group(Pid, T);
leave_group(Pid, Group) ->
	case count(Group, Pid) of
		0 -> ok;
		_ ->
			ets:delete_object(?GROUP_TABLE, ?LOCAL_RECORD(Group, Pid)),
			dec_group_counter(Group),
			dec_pid_counter(Pid)
	end.
	
drop_pid(Pid) ->
	Groups = local_groups(Pid),
	drop_pid_counter(Pid),
	lists:foreach(fun(Group) ->
				ets:delete_object(?GROUP_TABLE, ?LOCAL_RECORD(Group, Pid)),
				dec_group_counter(Group)
		end, Groups).

join_group(_Pid, []) -> ok;
join_group(Pid, [Group|T]) ->
	join_group(Pid, Group),
	join_group(Pid, T);
join_group(Pid, Group) ->
	case count(Group, Pid) of 
		0 -> 
			inc_pid_counter(Pid),
			inc_group_counter(Group),
			ets:insert(?GROUP_TABLE, ?LOCAL_RECORD(Group, Pid));
		_ -> ok
	end.

inc_pid_counter(Pid) ->
	try update_pid_counter(Pid, +1)
	catch _:_ ->
			Ref = erlang:monitor(process, Pid),
			ets:insert(?COUNTER_TABLE, ?PID_RECORD(Pid, Ref, 1))
	end.

inc_group_counter(Group) ->
	try update_group_counter(Group, +1)
	catch _:_ ->
			forward_request(Group),
			ets:insert(?COUNTER_TABLE, ?GROUP_RECORD(Group, 1))
	end.	
	
dec_group_counter(Group) ->
	case update_group_counter(Group, -1) of
		0 -> 
			ets:delete(?COUNTER_TABLE, ?GROUP_PK(Group)),
			cancel_request(Group);
		_ -> ok
	end.
			
dec_pid_counter(Pid) ->			
	case update_pid_counter(Pid, -1) of
		0 -> drop_pid_counter(Pid);
		_ -> ok
	end.
	
drop_pid_counter(Pid) ->
	case ets:take(?COUNTER_TABLE, ?PID_PK(Pid)) of
		[?PID_RECORD(_, Ref, _)] -> 
			erlang:demonitor(Ref);
		_ -> ok
	end.
	
count(Group, Pid) ->
	ets:select_count(?GROUP_TABLE, [{?LOCAL_RECORD(Group, Pid), [], [true]}]).
	
update_pid_counter(Pid, Value) ->
	update_counter(?PID_PK(Pid), 3, Value).
	
update_group_counter(Group, Value) ->
	update_counter(?GROUP_PK(Group), 2, Value).
	
update_counter(Key, Pos, Value) ->
	ets:update_counter(?COUNTER_TABLE, Key, {Pos, Value}).

forward_request([]) -> ok;
forward_request([Group|T]) -> 
	forward_request(Group),
	forward_request(T);
forward_request(Group) ->
	gen_server:abcast(nodes(), ?MODULE, {forward, Group, node()}).

cancel_request([]) -> ok;
cancel_request([Group|T]) -> 
	cancel_request(Group),
	cancel_request(T);
cancel_request(Group) ->
	gen_server:abcast(nodes(), ?MODULE, {cancel, Group, node()}).

check_drop(normal) -> drop();
check_drop(shutdown) -> drop();
check_drop({shutdown, _}) -> drop();
check_drop(_) -> ok.

drop() ->
	nomad_keeper:drop(?GROUP_TABLE),
	nomad_keeper:drop(?COUNTER_TABLE).

send_msg([], _Msg) -> ok;
send_msg([Pid | Pids], Msg) ->
	Pid ! Msg,
	send_msg(Pids, Msg).

remote_send([], _Group, _Msg) -> ok;
remote_send(Nodes, Group, Msg) -> 
	gen_server:abcast(Nodes, ?MODULE, {send, Group, Msg, node()}),
	ok.
