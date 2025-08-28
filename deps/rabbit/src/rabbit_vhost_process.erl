%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% This module implements a vhost identity process.

%% On start this process will try to recover the vhost data and
%% processes structure (queues and message stores).
%% If recovered successfully, the process will save it's PID
%% to the virtual host process registry. If the virtual host PID is in the registry and the
%% process is alive, then the vhost is considered to be running.

%% On termination, the ptocess will notify of vhost going down.

%% The process will also check periodically if the vhost is still
%% present in the database and stop the vhost supervision tree when it
%% disappears.

-module(rabbit_vhost_process).

-include_lib("kernel/include/logger.hrl").


-define(VHOST_CHECK_INTERVAL, 5000).

-behaviour(gen_server2).
-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

start_link(VHost) ->
    gen_server2:start_link(?MODULE, [VHost], []).


init([VHost]) ->
    process_flag(trap_exit, true),
    ?LOG_DEBUG("Recovering data for virtual host ~ts", [VHost]),
    try
        %% Recover the vhost data and save it to vhost registry.
        ok = rabbit_vhost:recover(VHost),
        rabbit_vhost_sup_sup:save_vhost_process(VHost, self()),
        _ = timer:send_interval(?VHOST_CHECK_INTERVAL, check_vhost),
        true = erlang:garbage_collect(),
        {ok, VHost}
    catch _:Reason:Stacktrace ->
        rabbit_amqqueue:mark_local_durable_queues_stopped(VHost),
        ?LOG_ERROR("Unable to recover vhost ~tp data. Reason ~tp~n"
                         " Stacktrace ~tp",
                         [VHost, Reason, Stacktrace]),
        {stop, Reason}
    end.

handle_call(_,_,VHost) ->
    {reply, ok, VHost}.

handle_cast(_, VHost) ->
    {noreply, VHost}.

handle_info(check_vhost, VHost) ->
    case rabbit_vhost:exists(VHost) of
        true  -> {noreply, VHost};
        false ->
            ?LOG_WARNING("Virtual host '~ts' is gone. "
                               "Stopping its top level supervisor.",
                               [VHost]),
            %% Stop vhost's top supervisor in a one-off process to avoid a deadlock:
            %% us (a child process) waiting for supervisor shutdown and our supervisor(s)
            %% waiting for us to shutdown.
            spawn(
                fun() ->
                    rabbit_vhost_sup_sup:stop_and_delete_vhost(VHost)
                end),
            {noreply, VHost}
    end;
handle_info(_, VHost) ->
    {noreply, VHost}.

terminate(shutdown, VHost) ->
    %% Notify that vhost is stopped.
    rabbit_vhost:vhost_down(VHost);
terminate(_, _VHost) ->
    ok.

code_change(_OldVsn, VHost, _Extra) ->
    {ok, VHost}.
