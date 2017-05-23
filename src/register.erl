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

-module(register).

%% ====================================================================
%% API functions
%% ====================================================================
-export([whereis_name/1, register_name/2, unregister_name/1, send/2]).

whereis_name(Pid) when is_pid(Pid) -> 
	Pid;
whereis_name({local, Name}) ->
	whereis_name(Name);
whereis_name({via, Mod, Name}) -> 
	Mod:whereis_name(Name);
whereis_name({global, Name}) -> 
	global:whereis_name(Name);
whereis_name(Name) when is_atom(Name) -> 
	whereis(Name);
whereis_name(_) -> 
	undefined.

register_name({local, Name}, Pid) ->
	register_name(Name, Pid);
register_name({via, Mod, Name}, Pid) ->
	case Mod:register_name(Name, Pid) of
		yes -> true;
		no -> false
	end;
register_name({global, Name}, Pid) ->
	case global:register_name(Name, Pid) of
		yes -> true;
		no -> false
	end;
register_name(Name, Pid) when is_atom(Name) ->
	try register(Name, Pid)
	catch _:_ -> false
	end;    
register_name(_Name, _Pid) -> 
	exit(badname).

unregister_name({local, Name}) ->
	unregister_name(Name);
unregister_name({via, Mod, Name}) ->
	Mod:unregister_name(Name),
	true;
unregister_name({global, Name}) ->
	global:unregister_name(Name),
	true;
unregister_name(Name) when is_atom(Name) ->
	try unregister(Name)
	catch _:_ -> false
	end;
unregister_name(_Name) -> 
	exit(badname).

send(Pid, Msg) when is_pid(Pid) -> 
	Pid ! Msg;
send({local, Name}, Msg) -> 
	send(Name, Msg);
send({via, Mod, Name}, Msg) -> 
	Mod:send(Name, Msg);
send({global, Name}, Msg) -> 
	global:send(Name, Msg);
send(Name, Msg) when is_atom(Name) ->
	case whereis(Name) of
		undefined -> exit({badarg, {Name, Msg}});
		Pid -> Pid ! Msg
	end;
send(Name, Msg) -> 
	exit({badarg, {Name, Msg}}).
