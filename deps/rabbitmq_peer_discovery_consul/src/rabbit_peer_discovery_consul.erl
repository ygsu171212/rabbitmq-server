%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% The Initial Developer of the Original Code is AWeber Communications.
%% Copyright (c) 2015-2016 AWeber Communications
%% Copyright (c) 2007-2024 Broadcom. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved. All rights reserved.
%%

-module(rabbit_peer_discovery_consul).
-behaviour(rabbit_peer_discovery_backend).

-include_lib("kernel/include/logger.hrl").
-include_lib("rabbitmq_peer_discovery_common/include/rabbit_peer_discovery.hrl").
-include("rabbit_peer_discovery_consul.hrl").

-export([init/0, list_nodes/0, supports_registration/0, register/0, unregister/0,
         post_registration/0, lock/1, unlock/1]).
-export([send_health_check_pass/0]).
-export([session_ttl_update_callback/1]).
%% for debugging from the REPL
-export([service_id/0, service_address/0]).
%% for debugging from the REPL
-export([http_options/1, http_options/2]).

%% for tests
-ifdef(TEST).
-compile(export_all).
-endif.

-define(CONFIG_MODULE, rabbit_peer_discovery_config).
-define(UTIL_MODULE,   rabbit_peer_discovery_util).

-define(CONSUL_CHECK_NOTES, "RabbitMQ Consul-based peer discovery plugin TTL check").

-define(META_KEY_CLUSTER_NAME, <<"cluster">>).
-define(META_KEY_ERLANG_NODENAME, <<"erlang-node-name">>).

%%
%% API
%%

init() ->
    ?LOG_DEBUG(
       "Peer discovery Consul: initialising...",
       #{domain => ?RMQLOG_DOMAIN_PEER_DISC}),
    ok = application:ensure_started(inets),
    %% we cannot start this plugin yet since it depends on the rabbit app,
    %% which is in the process of being started by the time this function is called
    _ = application:load(rabbitmq_peer_discovery_common),
    rabbit_peer_discovery_httpc:maybe_configure_proxy(),
    rabbit_peer_discovery_httpc:maybe_configure_inet6().

-spec list_nodes() -> {ok, {Nodes :: list(), NodeType :: rabbit_types:node_type()}} | {error, Reason :: string()}.

list_nodes() ->
    Fun0 = fun() -> {ok, {[], disc}} end,
    Fun1 = fun() ->
                   ?LOG_WARNING(
                      "Peer discovery backend is set to ~ts but final "
                      "config does not contain "
                      "rabbit.cluster_formation.peer_discovery_consul. "
                      "Cannot discover any nodes because Consul cluster "
                      "details are not configured!",
                      [?MODULE],
                      #{domain => ?RMQLOG_DOMAIN_PEER_DISC}),
                   {ok, {[], disc}}
           end,
    Fun2 = fun(Proplist) ->
                   case internal_lock() of
                       {ok, Priv} ->
                           try
                               M = maps:from_list(Proplist),
                               Path = rabbit_peer_discovery_httpc:build_path([v1, health, service, service_name()]),
                               HttpOpts = http_options(M),
                               case rabbit_peer_discovery_httpc:get(get_config_key(consul_scheme, M),
                                                                    get_config_key(consul_host, M),
                                                                    get_integer_config_key(consul_port, M),
                                                                    Path,
                                                                    list_nodes_query_args(),
                                                                    maybe_add_acl([]),
                                                                    HttpOpts) of
                                   {ok, Nodes} ->
                                       IncludeWithWarnings = get_config_key(consul_include_nodes_with_warnings, M),
                                       Result = extract_node(
                                                  sort_nodes(
                                                    filter_nodes(Nodes, IncludeWithWarnings))),
                                       {ok, {Result, disc}};
                                   {error, _} = Error ->
                                       Error
                               end
                           after
                               internal_unlock(Priv)
                           end;
                       {error, _} = Error ->
                           Error
                   end
           end,
    rabbit_peer_discovery_util:maybe_backend_configured(?BACKEND_CONFIG_KEY, Fun0, Fun1, Fun2).


-spec supports_registration() -> boolean().

supports_registration() ->
    true.


-spec register() -> ok | {error, Reason :: string()}.
register() ->
  M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
  case registration_body() of
    {ok, Body} ->
      ?LOG_DEBUG(
         "Consul registration body: ~ts", [Body],
         #{domain => ?RMQLOG_DOMAIN_PEER_DISC}),
      Path = rabbit_peer_discovery_httpc:build_path([v1, agent, service, register]),
      Headers = maybe_add_acl([]),
      HttpOpts = http_options(M),
      case rabbit_peer_discovery_httpc:put(get_config_key(consul_scheme, M),
                                           get_config_key(consul_host, M),
                                           get_integer_config_key(consul_port, M),
                                           Path,
                                           [],
                                           Headers,
                                           HttpOpts,
                                           Body) of
        {ok, _} -> ok;
        Error   -> Error
      end;
    Error -> Error
  end.


-spec unregister() -> ok | {error, Reason :: string()}.
unregister() ->
  M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
  ID = service_id(),
  ?LOG_DEBUG(
     "Unregistering with Consul using service ID '~ts'", [ID],
     #{domain => ?RMQLOG_DOMAIN_PEER_DISC}),
  Path = rabbit_peer_discovery_httpc:build_path([v1, agent, service, deregister, ID]),
  Headers = maybe_add_acl([]),
  HttpOpts = http_options(M),
  case rabbit_peer_discovery_httpc:put(get_config_key(consul_scheme, M),
                                       get_config_key(consul_host, M),
                                       get_integer_config_key(consul_port, M),
                                       Path,
                                       [],
                                       Headers,
                                       HttpOpts,
                                       []) of
    {ok, Response} ->
          ?LOG_INFO(
             "Consul's response to the unregistration attempt: ~tp",
             [Response],
             #{domain => ?RMQLOG_DOMAIN_PEER_DISC}),
          ok;
    Error   ->
          ?LOG_INFO(
             "Failed to unregister service with ID '~ts` with Consul: ~tp",
             [ID, Error],
             #{domain => ?RMQLOG_DOMAIN_PEER_DISC}),
          Error
  end.

-spec post_registration() -> ok.

post_registration() ->
    %% don't wait for one full interval, make
    %% sure we let Consul know the service is healthy
    %% right after registration. See rabbitmq/rabbitmq_peer_discovery_consul#8.
    send_health_check_pass(),
    ok.

-spec lock(Nodes :: [node()]) ->
    not_supported.

lock(_Nodes) ->
    not_supported.

-spec unlock(Data :: term()) -> ok.

unlock(_Data) ->
    ok.

-spec internal_lock() ->
    {ok, Data :: term()} | {error, Reason :: string()}.

internal_lock() ->
    M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
    ?LOG_DEBUG(
       "Effective Consul peer discovery configuration: ~tp", [M],
       #{domain => ?RMQLOG_DOMAIN_PEER_DISC}),
    Node = node(),
    case create_session(Node, get_config_key(consul_svc_ttl, M)) of
        {ok, SessionId} ->
            TRef = start_session_ttl_updater(SessionId),
            Now = erlang:system_time(seconds),
            EndTime = Now + get_config_key(lock_wait_time, M),
            lock(TRef, SessionId, Now, EndTime);
        {error, Reason} ->
            {error, lists:flatten(io_lib:format("Error while creating a session, reason: ~0p",
                                                [Reason]))}
    end.

-spec internal_unlock({SessionId :: string(), TRef :: timer:tref()}) -> ok.

internal_unlock({SessionId, TRef}) ->
    _ = timer:cancel(TRef),
    ?LOG_DEBUG(
       "Stopped session renewal",
       #{domain => ?RMQLOG_DOMAIN_PEER_DISC}),
    case release_lock(SessionId) of
        {ok, true} ->
            ok;
        {ok, false} ->
            {error, lists:flatten(io_lib:format("Error while releasing the lock, session ~ts may have been invalidated", [SessionId]))};
        {error, _} = Err ->
            Err
    end.

%%
%% Implementation
%%

-spec get_config_key(Key :: atom(), Map :: #{atom() => peer_discovery_config_value()})
                    -> peer_discovery_config_value().

get_config_key(Key, Map) ->
    ?CONFIG_MODULE:get(Key, ?CONFIG_MAPPING, Map).

-spec get_integer_config_key(Key :: atom(), Map :: #{atom() => peer_discovery_config_value()})
                            -> integer().

get_integer_config_key(Key, Map) ->
    ?CONFIG_MODULE:get_integer(Key, ?CONFIG_MAPPING, Map).


-spec http_options(Map :: #{atom() => peer_discovery_config_value()}) -> list().
http_options(M) ->
  case maps:get(ssl_options, M, none) of
    none -> [];
    []   -> [];
    Opts -> [{ssl, Opts}]
  end.


-spec http_options(HttpOpts0 :: list(), Map :: #{atom() => peer_discovery_config_value()}) -> list().
http_options(HttpOpts0, M) ->
  TLSOpts = case get_config_key(consul_scheme, M) of
    "http"  -> [];
    "https" ->
      case maps:get(ssl_options, M, none) of
        none -> [];
        []   -> [];
        Opts -> [{ssl, Opts}]
      end
  end,

  HttpOpts1 = [TLSOpts | HttpOpts0],
  HttpOpts1.

-spec filter_nodes(ConsulResult :: [#{term() => term()}], AllowWarning :: boolean()) -> [#{term() => term()}].
filter_nodes(Nodes, Warn) ->
  case Warn of
    true ->
      lists:filter(fun(Node) ->
                    Checks = maps:get(<<"Checks">>, Node),
                    lists:all(fun(Check) ->
                      lists:member(maps:get(<<"Status">>, Check),
                                   [<<"passing">>, <<"warning">>])
                              end,
                              Checks)
                   end,
                   Nodes);
    false -> Nodes
  end.

-spec sort_nodes(ConsulResult :: [#{binary() => term()}]) -> [#{binary() => term()}].
sort_nodes(Nodes) ->
  lists:sort(
    fun(NodeA, NodeB) ->
        IndexA = maps:get(
                   <<"CreateIndex">>,
                   maps:get(<<"Service">>, NodeA, #{}), undefined),
        IndexB = maps:get(
                   <<"CreateIndex">>,
                   maps:get(<<"Service">>, NodeB, #{}), undefined),
        %% `undefined' is always greater than an integer, so we are fine here.
        IndexA =< IndexB
    end, Nodes).

-spec extract_node(ConsulResult :: [#{binary() => term()}]) -> list().
extract_node([]) ->
    [];
extract_node([H | _]) ->
  Service = maps:get(<<"Service">>, H),
  Meta = maps:get(<<"Meta">>, Service, #{}),
  NodeName = case Meta of
    #{?META_KEY_ERLANG_NODENAME := Node} ->
      binary_to_atom(Node);
    _ ->
      Value = maps:get(<<"Address">>, Service),
      case ?UTIL_MODULE:as_string(Value) of
        "" ->
          NodeData = maps:get(<<"Node">>, H),
          Node = maps:get(<<"Node">>, NodeData),
          maybe_add_domain(?UTIL_MODULE:node_name(Node));
        Address ->
          ?UTIL_MODULE:node_name(Address)
      end
  end,
  NodeName.

-spec maybe_add_acl(QArgs :: list()) -> list().
maybe_add_acl(List) ->
  M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
  case get_config_key(consul_acl_token, M) of
    "undefined" -> List;
    ACL         -> [{"X-Consul-Token", ACL} | List]
  end.

-spec list_nodes_query_args() -> list().
list_nodes_query_args() ->
  M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
  list_nodes_query_args(get_config_key(cluster_name, M)).

-spec list_nodes_query_args(ClusterName :: string()) -> list().
list_nodes_query_args(Cluster) ->
  ClusterTag = case Cluster of
    "default" -> [];
    _         -> [{tag, Cluster}]
  end,
  M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
  list_nodes_query_args(ClusterTag, get_config_key(consul_include_nodes_with_warnings, M)).

-spec list_nodes_query_args(Args :: list(), AllowWarn :: atom()) -> list().
list_nodes_query_args(Value, Warn) ->
    case Warn of
        true  -> Value;
        false -> [passing | Value]
    end.

-spec registration_body() -> {ok, Body :: binary()} | {error, atom()}.
registration_body() ->
  Payload = build_registration_body(),
  registration_body(rabbit_json:try_encode(Payload)).

-spec registration_body(Response :: {ok, Body :: string()} |
                                    {error, Reason :: atom()})
  -> {ok, Body :: binary()} | {error, Reason :: atom()}.
registration_body({ok, Body}) ->
  {ok, rabbit_data_coercion:to_binary(Body)};
registration_body({error, Reason}) ->
  ?LOG_ERROR(
     "Error serializing the request body: ~tp",
     [Reason],
     #{domain => ?RMQLOG_DOMAIN_PEER_DISC}),
  {error, Reason}.


-spec build_registration_body() -> list().
build_registration_body() ->
  Payload1 = registration_body_add_id(),
  Payload2 = registration_body_add_name(Payload1),
  Payload3 = registration_body_maybe_add_address(Payload2),
  Payload4 = registration_body_add_port(Payload3),
  Payload5 = registration_body_maybe_add_check(Payload4),
  Payload6 = registration_body_maybe_add_tag(Payload5),
  registration_body_maybe_add_meta(Payload6).

-spec registration_body_add_id() -> list().
registration_body_add_id() ->
  [{'ID', rabbit_data_coercion:to_atom(service_id())}].

-spec registration_body_add_name(Payload :: list()) -> list().
registration_body_add_name(Payload) ->
  Name = rabbit_data_coercion:to_atom(service_name()),
  lists:append(Payload, [{'Name', Name}]).

-spec registration_body_maybe_add_address(Payload :: list())
    -> list().
registration_body_maybe_add_address(Payload) ->
  registration_body_maybe_add_address(Payload, service_address()).

-spec registration_body_maybe_add_address(Payload :: list(), string())
    -> list().
registration_body_maybe_add_address(Payload, "undefined") -> Payload;
registration_body_maybe_add_address(Payload, Address) ->
  lists:append(Payload, [{'Address', rabbit_data_coercion:to_atom(Address)}]).

registration_body_maybe_add_check(Payload) ->
  M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
  TTL = get_config_key(consul_svc_ttl, M),
  registration_body_maybe_add_check(Payload, TTL).

-spec registration_body_maybe_add_check(Payload :: list(),
                                        TTL :: integer() | undefined)
    -> list().
registration_body_maybe_add_check(Payload, undefined) ->
    case registration_body_maybe_add_deregister([]) of
        [{'DeregisterCriticalServiceAfter', _}]->
            ?LOG_WARNING(
               "Can't use Consul's service deregistration feature without "
               "using TTL. The parameter  will be ignored",
               #{domain => ?RMQLOG_DOMAIN_PEER_DISC}),
            Payload;

        _ -> Payload
    end;
registration_body_maybe_add_check(Payload, TTL) ->
    CheckItems = [{'Notes', rabbit_data_coercion:to_atom(?CONSUL_CHECK_NOTES)},
        {'TTL', rabbit_data_coercion:to_atom(service_ttl(TTL))},
        {'Status', 'passing'}],
    Check = [{'Check',  registration_body_maybe_add_deregister(CheckItems)}],
    lists:append(Payload, Check).

-spec registration_body_add_port(Payload :: list()) -> list().
registration_body_add_port(Payload) ->
  M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
  lists:append(Payload,
               [{'Port', get_config_key(consul_svc_port, M)}]).

registration_body_maybe_add_deregister(Payload) ->
    M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
    Deregister = get_config_key(consul_deregister_after, M),
    registration_body_maybe_add_deregister(Payload, Deregister).

-spec registration_body_maybe_add_deregister(Payload :: list(),
    TTL :: integer() | undefined)
        -> list().
registration_body_maybe_add_deregister(Payload, undefined) -> Payload;
registration_body_maybe_add_deregister(Payload, Deregister_After) ->
    Deregister = {'DeregisterCriticalServiceAfter',
        rabbit_data_coercion:to_atom(service_ttl(Deregister_After))},
    Payload ++ [Deregister].

-spec registration_body_maybe_add_tag(Payload :: list()) -> list().
registration_body_maybe_add_tag(Payload) ->
  M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
  Value = get_config_key(cluster_name, M),
  Tags = ?UTIL_MODULE:as_list(get_config_key(consul_svc_tags, M)),
  registration_body_maybe_add_tag(Payload, Value, Tags).

-spec registration_body_maybe_add_tag(Payload :: list(),
                                      ClusterName :: string(),
                                      Tags :: list())
    -> list().
registration_body_maybe_add_tag(Payload, "default", []) -> Payload;
registration_body_maybe_add_tag(Payload, "default", Tags) ->
  lists:append(Payload, [{'Tags', [rabbit_data_coercion:to_atom(X) || X <- Tags]}]);
registration_body_maybe_add_tag(Payload, Cluster, []) ->
  lists:append(Payload, [{'Tags', [rabbit_data_coercion:to_atom(Cluster)]}]);
registration_body_maybe_add_tag(Payload, Cluster, Tags) ->
  lists:append(Payload, [{'Tags', [rabbit_data_coercion:to_atom(Cluster)] ++ [rabbit_data_coercion:to_atom(X) || X <- Tags]}]).


-spec registration_body_maybe_add_meta(Payload :: list()) -> list().
registration_body_maybe_add_meta(Payload) ->
    M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
    Meta0 = ?UTIL_MODULE:as_list(get_config_key(consul_svc_meta, M)),
    Meta1 = maps:from_list(Meta0),
    Meta2 = Meta1#{?META_KEY_ERLANG_NODENAME => atom_to_binary(node())},
    Meta3 = case get_config_key(cluster_name, M) of
                "default" ->
                    Meta2;
                ClusterName ->
                    ClusterName1 = rabbit_data_coercion:to_binary(ClusterName),
                    Meta2#{?META_KEY_CLUSTER_NAME => ClusterName1}
            end,
    Merged = maps:to_list(Meta3),
    lists:append(Payload, [{'Meta', Merged}]).

-spec validate_addr_parameters(false | true, false | true) -> false | true.
validate_addr_parameters(false, true) ->
    ?LOG_WARNING(
       "The parameter CONSUL_SVC_ADDR_NODENAME"
       " can be used only if CONSUL_SVC_ADDR_AUTO is true."
       " CONSUL_SVC_ADDR_NODENAME value will be ignored.",
       #{domain => ?RMQLOG_DOMAIN_PEER_DISC}),
    false;
validate_addr_parameters(_, _) ->
    true.


-spec service_address() -> string().
service_address() ->
  M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
  validate_addr_parameters(get_config_key(consul_svc_addr_auto, M),
      get_config_key(consul_svc_addr_nodename, M)),
  service_address(get_config_key(consul_svc_addr, M),
                  get_config_key(consul_svc_addr_auto, M),
                  get_config_key(consul_svc_addr_nic, M),
                  get_config_key(consul_svc_addr_nodename, M)).


-spec service_address(Static :: string(),
                      Auto :: boolean(),
                      AutoNIC :: string(),
                      FromNodename :: boolean()) -> string().
service_address(_, true, "undefined", FromNodename) ->
  rabbit_peer_discovery_util:node_hostname(FromNodename);
service_address(Value, false, "undefined", _) ->
  Value;
service_address(_, true, NIC, _) ->
  %% TODO: support IPv6
  {ok, Addr} = rabbit_peer_discovery_util:nic_ipv4(NIC),
  Addr;
%% this combination makes no sense but this is what rabbitmq-autocluster
%% and this plugin have been allowing for a couple of years, so we keep
%% this clause around for backwards compatibility.
%% See rabbitmq/rabbitmq-peer-discovery-consul#12 for details.
service_address(_, false, NIC, _) ->
  {ok, Addr} = rabbit_peer_discovery_util:nic_ipv4(NIC),
  Addr.


-spec service_id() -> string().
service_id() ->
  M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
  case get_config_key(consul_svc_id, M) of
      "undefined" ->
          service_id(get_config_key(consul_svc, M),
                     service_address());
      ID ->
          ID
  end.

-spec service_id(Name :: string(), Address :: string()) -> string().
service_id(Service, "undefined") -> Service;
service_id(Service, Address) ->
  string:join([Service, Address], ":").

-spec service_name() -> string().
service_name() ->
  M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
  get_config_key(consul_svc, M).

-spec service_ttl(TTL :: integer()) -> string().
service_ttl(Value) ->
  rabbit_peer_discovery_util:as_string(Value) ++ "s".

-spec maybe_add_domain(Domain :: atom()) -> atom().
maybe_add_domain(Value) ->
  M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
  case rabbit_nodes:name_type() of
      longnames ->
          rabbit_data_coercion:to_atom(string:join([atom_to_list(Value),
                                    "node",
                                    get_config_key(consul_domain, M)],
                                   "."));
      shortnames -> Value
  end.

%%--------------------------------------------------------------------
%% @doc
%% Let Consul know that this node is still around
%% @end
%%--------------------------------------------------------------------

-spec send_health_check_pass() -> ok.

send_health_check_pass() ->
  Service = string:join(["service", service_id()], ":"),
  M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
  ?LOG_DEBUG(
     "Running Consul health check",
     #{domain => ?RMQLOG_DOMAIN_PEER_DISC}),
  Path = rabbit_peer_discovery_httpc:build_path([v1, agent, check, pass, Service]),
  Headers = maybe_add_acl([]),
  HttpOpts = http_options(M),
  case rabbit_peer_discovery_httpc:put(get_config_key(consul_scheme, M),
                                       get_config_key(consul_host, M),
                                       get_integer_config_key(consul_port, M),
                                       Path,
                                       [],
                                       Headers,
                                       HttpOpts,
                                       []) of
    {ok, []} -> ok;
    {error, "429"} ->
          %% Too Many Requests, see https://www.consul.io/docs/agent/checks.html
          ?LOG_WARNING(
             "Consul responded to a health check with 429 Too Many Requests",
             #{domain => ?RMQLOG_DOMAIN_PEER_DISC}),
          ok;
    %% starting with Consul 1.11, see https://github.com/hashicorp/consul/pull/11950
    {error, "404"} ->
      ?LOG_WARNING(
         "Consul responded to a health check with a 404 status, will "
         "wait and try re-registering",
         #{domain => ?RMQLOG_DOMAIN_PEER_DISC}),
      maybe_re_register(wait_for_list_nodes()),
      ok;
    %% prior to Consul 1.11, see https://github.com/hashicorp/consul/pull/11950
    {error, "500"} ->
          ?LOG_WARNING(
             "Consul responded to a health check with a 500 status, will "
             "wait and try re-registering",
             #{domain => ?RMQLOG_DOMAIN_PEER_DISC}),
          maybe_re_register(wait_for_list_nodes()),
          ok;
    {error, Reason} ->
          ?LOG_ERROR(
             "Error running Consul health check: ~tp",
             [Reason],
             #{domain => ?RMQLOG_DOMAIN_PEER_DISC}),
      ok
  end.

maybe_re_register({error, Reason}) ->
    ?LOG_ERROR(
       "Internal error in Consul while updating health check. "
       "Cannot obtain list of nodes registered in Consul either: ~tp",
       [Reason],
       #{domain => ?RMQLOG_DOMAIN_PEER_DISC});
maybe_re_register({ok, {Members, _NodeType}}) ->
    maybe_re_register(Members);
maybe_re_register(Members) ->
    case lists:member(node(), Members) of
        true ->
            ?LOG_ERROR(
               "Internal error in Consul while updating health check",
               #{domain => ?RMQLOG_DOMAIN_PEER_DISC});
        false ->
            ?LOG_ERROR(
               "Internal error in Consul while updating health check, "
               "node is not registered. Re-registering",
               #{domain => ?RMQLOG_DOMAIN_PEER_DISC}),
            register()
    end.

wait_for_list_nodes() ->
    wait_for_list_nodes(60).

-spec wait_for_list_nodes(non_neg_integer()) -> {'ok', term()} | {'error', term()}.
wait_for_list_nodes(0) ->
    list_nodes();
wait_for_list_nodes(N) ->
    case list_nodes() of
        {ok, _} = Reply ->
            Reply;
        _ ->
            timer:sleep(1000),
            wait_for_list_nodes(N - 1)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Create a session to be acquired for a common key
%% @end
%%--------------------------------------------------------------------
-spec create_session(atom(), pos_integer()) -> {ok, string()} | {error, Reason::any()}.
create_session(Name, TTL) ->
    case consul_session_create([], maybe_add_acl([]),
                               [{'Name', Name},
                                {'TTL', rabbit_data_coercion:to_atom(service_ttl(TTL))}]) of
        {ok, Response} ->
            {ok, get_session_id(Response)};
        {error, _} = Err ->
            Err
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Create session
%% @end
%%--------------------------------------------------------------------
-spec consul_session_create(Query, Headers, Body) -> {ok, term()} | {error, any()} when
      Query :: list(),
      Headers :: [{string(), string()}],
      Body :: thoas:input_term().
consul_session_create(Query, Headers, Body) ->
    M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
    case serialize_json_body(Body) of
        {ok, Serialized} ->
            HttpOpts = http_options(M),
            rabbit_peer_discovery_httpc:put(get_config_key(consul_scheme, M),
                                            get_config_key(consul_host, M),
                                            get_integer_config_key(consul_port, M),
                                            "v1/session/create",
                                            Query,
                                            Headers,
                                            HttpOpts,
                                            Serialized);
        {error, _} = Err ->
            Err
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Process the result of JSON encoding the request body payload,
%% returning the body as a binary() value or the error returned by
%% the JSON serialization library.
%% @end
%%--------------------------------------------------------------------
-spec serialize_json_body(thoas:input_term()) -> {ok, Payload :: binary()} | {error, atom()}.
serialize_json_body([]) -> {ok, []};
serialize_json_body(Payload) ->
    case rabbit_json:try_encode(Payload) of
        {ok, Body} -> {ok, Body};
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Extract session ID from Consul response
%% @end
%%--------------------------------------------------------------------
-spec get_session_id(#{binary() => term()}) -> string().
get_session_id(#{<<"ID">> := ID}) -> binary:bin_to_list(ID).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Start periodically renewing an existing session ttl
%% @end
%%--------------------------------------------------------------------
-spec start_session_ttl_updater(string()) -> timer:tref().
start_session_ttl_updater(SessionId) ->
    M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
    Interval = get_config_key(consul_svc_ttl, M),
    ?LOG_DEBUG(
       "Starting session renewal",
       #{domain => ?RMQLOG_DOMAIN_PEER_DISC}),
    {ok, TRef} = timer:apply_interval(Interval * 500, ?MODULE,
                                      session_ttl_update_callback, [SessionId]),
    TRef.

%%
%% @doc
%% Tries to acquire lock. If the lock is held by someone else, waits until it
%% is released, or too much time has passed
%% @end
-spec lock(timer:tref(), string(), pos_integer(), pos_integer()) -> {ok, {SessionId :: string(), TRef :: timer:tref()}} | {error, string()}.
lock(TRef, _, Now, EndTime) when EndTime < Now ->
    _ = timer:cancel(TRef),
    {error, "Acquiring lock taking too long, bailing out"};
lock(TRef, SessionId, _, EndTime) ->
    case acquire_lock(SessionId) of
        {ok, true} ->
            {ok, {SessionId, TRef}};
        {ok, false} ->
            case get_lock_status() of
                {ok, {SessionHeld, ModifyIndex}} ->
                    Wait = max(EndTime - erlang:system_time(seconds), 0),
                    case wait_for_lock_release(SessionHeld, ModifyIndex, Wait) of
                        ok ->
                            lock(TRef, SessionId, erlang:system_time(seconds), EndTime);
                        {error, Reason} ->
                            _ = timer:cancel(TRef),
                            {error, lists:flatten(io_lib:format("Error waiting for lock release, reason: ~ts",[Reason]))}
                    end;
                {error, Reason} ->
                    _ = timer:cancel(TRef),
                    {error, lists:flatten(io_lib:format("Error obtaining lock status, reason: ~ts", [Reason]))}
            end;
        {error, Reason} ->
            _ = timer:cancel(TRef),
            {error, lists:flatten(io_lib:format("Error while acquiring lock, reason: ~ts", [Reason]))}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Acquire session for a key
%% @end
%%--------------------------------------------------------------------
-spec acquire_lock(string()) -> {ok, any()} | {error, string()}.
acquire_lock(SessionId) ->
    consul_kv_write(startup_lock_path(), [{acquire, SessionId}], maybe_add_acl([]), []).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Release a previously acquired lock held by a given session
%% @end
%%--------------------------------------------------------------------
-spec release_lock(string()) -> {ok, any()} | {error, string()}.
release_lock(SessionId) ->
    consul_kv_write(startup_lock_path(), [{release, SessionId}], maybe_add_acl([]), []).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Write KV store key value
%% @end
%%--------------------------------------------------------------------
-spec consul_kv_write(Path, Query, Headers, Body) -> {ok, any()} | {error, string()} when
      Path :: string(),
      Query :: [{string() | atom(), string()}],
      Headers :: [{string(), string()}],
      Body :: term().
consul_kv_write(Path, Query, Headers, Body) ->
    M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
    case serialize_json_body(Body) of
        {ok, Serialized} ->
            HttpOpts = http_options(M),
            rabbit_peer_discovery_httpc:put(get_config_key(consul_scheme, M),
                                            get_config_key(consul_host, M),
                                            get_integer_config_key(consul_port, M),
                                            "v1/kv/" ++ Path,
                                            Query,
                                            Headers,
                                            HttpOpts,
                                            Serialized);
        {error, _} = Err ->
            Err
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Read KV store key value
%% @end
%%--------------------------------------------------------------------
-spec consul_kv_read(Path, Query, Headers) -> {ok, term()} | {error, string()} when
      Path :: string(),
      Query :: [{string(), string()}],
      Headers :: [{string(), string()}].
consul_kv_read(Path, Query, Headers) ->
    M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
    HttpOpts = http_options(M),
    rabbit_peer_discovery_httpc:get(get_config_key(consul_scheme, M),
                                    get_config_key(consul_host, M),
                                    get_integer_config_key(consul_port, M),
                                    "v1/kv/" ++ Path,
                                    Query,
                                    Headers,
                                    HttpOpts).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Get lock status
%% XXX: probably makes sense to wrap output in a record to be
%% more future-proof
%% @end
%%--------------------------------------------------------------------
-spec get_lock_status() -> {ok, term()} | {error, string()}.
get_lock_status() ->
    case consul_kv_read(startup_lock_path(), [], maybe_add_acl([])) of
        {ok, [KeyData | _]} ->
            SessionHeld = maps:get(<<"Session">>, KeyData, undefined) =/= undefined,
            ModifyIndex = maps:get(<<"ModifyIndex">>, KeyData),
            {ok, {SessionHeld, ModifyIndex}};
        {error, _} = Err ->
            Err
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns consul path for startup lock
%% @end
%%--------------------------------------------------------------------
-spec startup_lock_path() -> string().
startup_lock_path() ->
    base_path() ++ "/" ++ "startup_lock".

%%--------------------------------------------------------------------
%% @private
%% @doc Return a list of path segments that are the base path for all
%% consul kv keys related to current cluster.
%% @end
%%--------------------------------------------------------------------
-spec base_path() -> string().
base_path() ->
    M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
    Segments = [get_config_key(consul_lock_prefix, M), get_config_key(cluster_name, M)],
    rabbit_peer_discovery_httpc:build_path(Segments).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Wait for lock to be released if it has been acquired by another node
%% @end
%%--------------------------------------------------------------------
-spec wait_for_lock_release(atom(), pos_integer(), pos_integer()) -> ok | {error, string()}.
wait_for_lock_release(false, _, _) -> ok;
wait_for_lock_release(_, Index, Wait) ->
    case consul_kv_read(startup_lock_path(),
                        [{"index", Index}, {"wait", service_ttl(Wait)}],
                        maybe_add_acl([])) of
        {ok, _}          -> ok;
        {error, _} = Err -> Err
    end.

%%--------------------------------------------------------------------
%% @doc
%% Renew an existing session
%% @end
%%--------------------------------------------------------------------
-spec session_ttl_update_callback(string()) -> string().
session_ttl_update_callback(SessionId) ->
    _ = consul_session_renew(SessionId, [], maybe_add_acl([])),
    SessionId.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Renew session TTL
%% @end
%%--------------------------------------------------------------------
-spec consul_session_renew(string(), [{string(), string()}], [{string(), string()}]) -> {ok, term()} | {error, string()}.
consul_session_renew(SessionId, Query, Headers) ->
    M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
    Path = rabbit_peer_discovery_httpc:build_path([v1, session, renew, rabbit_data_coercion:to_atom(SessionId)]),
    HttpOpts = http_options(M),
    rabbit_peer_discovery_httpc:put(get_config_key(consul_scheme, M),
                                    get_config_key(consul_host, M),
                                    get_integer_config_key(consul_port, M),
                                    Path,
                                    Query,
                                    Headers,
                                    HttpOpts,
                                    []).
