%% -------------------------------------------------------------------
%%
%% riak_core: Core Riak Application
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc see riak_core_claim.erl

-module(riak_core_claim_2_dcs).
-export([default_choose_claim/1, default_choose_claim/2]).
-export([choose_claim_v2/1, choose_claim_v2/2]).

%% ===================================================================
%% API
%% ===================================================================

%% @spec default_choose_claim(riak_core_ring()) -> riak_core_ring()
%% @doc Choose a partition at random.
default_choose_claim(Ring) ->
    default_choose_claim(Ring, node()).

default_choose_claim(Ring, Node) ->
    choose_claim_v2(Ring, Node).

% this function is very inefficient since it always rebalances
% the ring. not a good idea using it in production environments.
choose_claim_v2(Ring) ->
    choose_claim_v2(Ring, node()).

choose_claim_v2(RingIn, _Node) ->
    Ring = riak_core_ring:upgrade(RingIn),
    claim_rebalance_2_dcs(Ring).

%% @private
claim_rebalance_2_dcs(Ring0) ->
    Ring = riak_core_ring:upgrade(Ring0),
    {DC10, DC20} = lists:partition(
            fun(Node) ->
                lists:prefix("dc1", atom_to_list(Node))
            end,
            riak_core_ring:claiming_members(Ring)),
    Min = min(length(DC10), length(DC20)),
    % one of the rest sublists is empty
    {DC1, Rest1} = lists:split(Min, DC10),
    {DC2, Rest2} = lists:split(Min, DC20),
    Nodes = lists:flatten(
                lists:map(fun(X) -> tuple_to_list(X) end,
                          lists:zip(DC1, DC2)),
                Rest1 ++ Rest2),
    Zipped = riak_core_claim:diagonal_stripe(Ring, Nodes),
    lists:foldl(fun({P, N}, Acc) ->
                        riak_core_ring:transfer_node(P, N, Acc)
                end,
                Ring,
                Zipped).
