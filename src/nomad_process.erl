%%
%% Copyright 2019 Joaquim Rocha <jrocha@gmailbox.org>
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

-module(nomad_process).

%% ====================================================================
%% API functions
%% ====================================================================
-export([sort/1]).

sort([]) -> [];
sort(List = [_]) -> List;
sort(List) ->
  NormalizedNode = normalize(node()),
  DistanceList = lists:map(
    fun(Pid) ->
      Node = node(Pid),
      Distance = distance_to(Node, NormalizedNode),
      {Distance, Pid}
    end, List),
  SortedList = lists:sort(fun is_near_than/2, DistanceList),
  [Pid || {_, Pid} <- SortedList].

is_near_than(A, B) when A < B -> true;
is_near_than(_, _) -> false.

%% 0 - same node
%% 1 - same machine
%% 2 - same domain
%% 3 - other
distance_to(Node, {Node, _Machine, _Domain}) -> 0;
distance_to(Node, Local) when is_atom(Node) ->
  NormalizedNode = normalize(Node),
  distance_to(NormalizedNode, Local);
distance_to({Node, _Machine, _Domain}, {Node, _Machine, _Domain}) -> 0;
distance_to({_RN, Machine, Domain}, {_LN, Machine, Domain}) -> 1;
distance_to({_RN, _RM, Domain}, {_LN, _LM, Domain}) -> 2;
distance_to(_Node, _Local) -> 3.

normalize(Node) ->
  Binary = atom_to_binary(Node, utf8),
  case binary:split(Binary, <<"@">>) of
    [_] -> {Node, <<>>, <<>>};
    [_, Path] ->
      case binary:split(Path, <<".">>) of
        [Machine] -> {Node, Machine, <<>>};
        [Machine, Domain] -> {Node, Machine, Domain}
      end
  end.

