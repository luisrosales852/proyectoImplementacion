%%% TC2037 - Implementation of Computational Methods
%%% Activity 6.2 - Distributed Programming in Erlang
%%% Distributed Taxi Rental System for Travel to an Airport
%%%
%%% MODULE: taxi
%%%
%%% TEAM MEMBERS:
%%% Author: Luis Alvaro Rosales Salazar - A01255674
%%% Author: Gabriel Rosendo Fuente Escalante - A01660266
%%% Author: Eduardo Didier Aguilar Alvarez - A00841850
%%% Author: Brian Roberto Gomez Martinez - A00841404
%%%
%%% PURPOSE
%%%   Each registered taxi is its own process that holds its status
%%%   (available / occupied) and location, answers the center's status
%%%   queries, receives trip offers and runs the transport service.
%%%
%%% PROCESS MODEL  (PID-based, with a local name per taxi)
%%%   * register_taxi/2 spawns the taxi process and RETURNS its PID. Once the
%%%     center confirms registration the process ALSO registers itself locally
%%%     under its TaxiId atom, so every operator command can address the taxi
%%%     by its id exactly as the spec writes it -- e.g.
%%%     taxi:accept_trip(taxi1, TripId). Passing the PID still works (the `!`
%%%     operator accepts a registered name OR a PID).
%%%   * The taxi monitors the center (erlang:monitor): if the center goes
%%%     DOWN (closed or crashed) the taxi stops itself automatically.
%%%   * The center monitors the taxi and tracks it by PID, so if the taxi
%%%     stops, the center just drops its entry (the local name is freed
%%%     automatically when the process ends).
%%%   * accept_trip/reject_trip include the TaxiId in the message they make
%%%     the taxi send to the center, so the center can confirm the CORRECT
%%%     taxi is responding to the offer; on accept the taxi marks itself
%%%     occupied, remembers the trip and notifies the center.
%%%
%%% HOW TO RUN AND TEST: see the "HOW TO RUN AND TEST" comment block at the
%%% top of center.erl for the per-node deployment steps and the full command
%%% sequence that exercises every interface function of all three modules.
-module(taxi).

%% Public API (run in the CALLER's process, using the taxi's PID).
-export([register_taxi/2, current_location/2,
         accept_trip/2, reject_trip/2,
         service_started/1, service_completed/1,
         remove_taxi/1, consult_taxi/1]).

%% Spawned entry point.
-export([init/4]).

%% Taxi state is a plain hardcoded 7-element list (no record). The fixed
%% positions are, in order:
%%   element 1 = id       atom label
%%   element 2 = loc      {X,Y}
%%   element 3 = status   available | occupied
%%   element 4 = airport  {X,Y} cached at registration
%%   element 5 = pending  {TripId, Origin} of an outstanding offer, or none
%%   element 6 = trip     {TripId, Origin} of the current trip, or none
%%   element 7 = center   center PID
%% Reads use lists:nth(N, T); updates rebuild the whole list so the comment
%% on each line shows exactly which element is being kept or changed.

%%% PUBLIC API

%% Register a taxi at the center. Returns the taxi's PID. The center is found
%% by its cluster-wide `global` name, so no PID is passed; the node must be
%% connected to the center node first (e.g. net_adm:ping/1).
register_taxi(TaxiId, InitialLocation) ->
    case global:whereis_name(center) of
        undefined ->
            io:format("Taxi ~p not registered: no center reachable "
                      "(connect to the center node first).~n", [TaxiId]),
            {error, no_center};
        CenterPid ->
            register_taxi(TaxiId, InitialLocation, CenterPid)
    end.

register_taxi(TaxiId, InitialLocation, CenterPid) ->
    Parent = self(),
    Pid = spawn(taxi, init, [TaxiId, InitialLocation, CenterPid, Parent]),
    receive
        {taxi_ready, Pid} ->
            io:format("Taxi ~p registered at ~p, PID ~p (node ~p).~n",
                      [TaxiId, InitialLocation, Pid, node()]),
            Pid;
        {taxi_error, Reason} ->
            io:format("Taxi ~p not registered: ~p~n", [TaxiId, Reason]),
            {error, Reason}
    after 10000 ->
        {error, timeout}
    end.

%% Update a taxi's location. TaxiId is the atom id (a PID also works).
current_location(TaxiId, Location) ->
    log_send(io_lib:format("update location to ~p", [Location])),
    TaxiId ! {update_loc, Location},
    ok.

%% Accept / reject the trip offer TripId (answers the center via the taxi).
%% TaxiId is the atom id (a PID also works); the taxi forwards its id to the
%% center inside the response so the center knows which taxi is replying.
accept_trip(TaxiId, TripId) ->
    log_send(io_lib:format("accept trip ~p", [TripId])),
    TaxiId ! {accept, TripId},
    ok.

reject_trip(TaxiId, TripId) ->
    log_send(io_lib:format("reject trip ~p", [TripId])),
    TaxiId ! {reject, TripId},
    ok.

%% Notify the center that the service started (taxi occupied at the origin).
service_started(TaxiId) ->
    sync(TaxiId, start, "Taxi started its service.").

%% Notify the center that the service finished (taxi available at airport).
service_completed(TaxiId) ->
    sync(TaxiId, complete, "Taxi completed its service (available at airport).").

%% Remove the taxi (only allowed when available).
remove_taxi(TaxiId) ->
    sync(TaxiId, remove, "Taxi removed.").

%% Check a taxi's status and location. TaxiId is the atom id (a PID also works).
consult_taxi(TaxiId) ->
    Ref = make_ref(),
    log_send("consult taxi"),
    TaxiId ! {consult, self(), Ref},
    receive
        {taxi_info, Ref, Id, Status, Loc} ->
            io:format("~nTaxi ~p | status: ~p | location: ~p~n", [Id, Status, Loc]),
            {ok, Status, Loc}
    after 10000 -> {error, timeout}
    end.

%% Shared synchronous command (start | complete | remove). TaxiId is the
%% atom id (a PID also works).
sync(TaxiId, Cmd, OkMsg) ->
    Ref = make_ref(),
    log_send(io_lib:format("~p command to taxi", [Cmd])),
    TaxiId ! {Cmd, self(), Ref},
    receive
        {result, Ref, ok}             -> io:format("~s~n", [OkMsg]), ok;
        {result, Ref, {error, Why}}   -> io:format("Command failed: ~p~n", [Why]),
                                         {error, Why}
    after 10000 -> {error, timeout}
    end.

%%% PROCESS LOOP

init(TaxiId, Loc, CenterPid, Parent) ->
    erlang:monitor(process, CenterPid),     % center dies -> we stop ourselves
    log_send(io_lib:format("register taxi ~p at ~p", [TaxiId, Loc])),
    CenterPid ! {register_taxi, TaxiId, self(), Loc},
    receive
        {registered, Airport} ->
            log_recv(io_lib:format("registration ack, airport at ~p", [Airport])),
            %% Register locally under the TaxiId so operator commands can
            %% address this taxi by its id, e.g. taxi:accept_trip(TaxiId, ...).
            %% The name is freed automatically when this process ends.
            case catch register(TaxiId, self()) of
                true ->
                    Parent ! {taxi_ready, self()},
                    %% Build the taxi state as a plain 7-element list:
                    %%   [id, loc, status, airport, pending, trip, center]
                    loop([TaxiId,      % element 1 = id
                          Loc,         % element 2 = loc
                          available,   % element 3 = status
                          Airport,     % element 4 = airport
                          none,        % element 5 = pending
                          none,        % element 6 = trip
                          CenterPid]); % element 7 = center
                _ ->
                    %% Name already in use on this node: abort cleanly. The
                    %% center's monitor fires 'DOWN' and drops the entry it
                    %% just added.
                    Parent ! {taxi_error, {name_in_use, TaxiId}}
            end;
        {register_error, Reason} ->
            Parent ! {taxi_error, Reason}
    after 10000 ->
        Parent ! {taxi_error, no_center_ack}
    end.

loop(T) ->
    receive
        %% Center asks for live status + location.
        {get_state, From, Ref} ->
            log_recv("status query from center"),
            log_send(io_lib:format("status ~p at ~p to center",
                                   [lists:nth(3, T), lists:nth(2, T)])), % element 3 = status, element 2 = loc
            From ! {taxi_state, Ref,
                    lists:nth(1, T),   % element 1 = id
                    lists:nth(3, T),   % element 3 = status
                    lists:nth(2, T)},  % element 2 = loc
            loop(T);

        %% Trip offer (informational; remember it so accept/reject can use it).
        {offer, TripId, Name, Origin} ->
            log_recv(io_lib:format("trip offer ~p from ~p (origin ~p)",
                                   [TripId, Name, Origin])),
            io:format("   -> respond: taxi:accept_trip(~p,~p) OR "
                      "taxi:reject_trip(~p,~p)~n",
                      [lists:nth(1, T), TripId, lists:nth(1, T), TripId]), % element 1 = id
            %% rebuild, only changing element 5 = pending
            loop([lists:nth(1, T),   % element 1 = id
                  lists:nth(2, T),   % element 2 = loc
                  lists:nth(3, T),   % element 3 = status
                  lists:nth(4, T),   % element 4 = airport
                  {TripId, Origin},  % element 5 = pending (updated)
                  lists:nth(6, T),   % element 6 = trip
                  lists:nth(7, T)]); % element 7 = center

        %% Operator accepts the pending offer.
        {accept, TripId} ->
            case lists:nth(5, T) of   % element 5 = pending
                {TripId, Origin} ->
                    log_send(io_lib:format("accept trip ~p to center", [TripId])),
                    lists:nth(7, T) ! {trip_response, accept, TripId,
                                       lists:nth(1, T), self()}, % element 7 = center, element 1 = id
                    %% rebuild, changing element 3 = status, 5 = pending, 6 = trip
                    loop([lists:nth(1, T),   % element 1 = id
                          lists:nth(2, T),   % element 2 = loc
                          occupied,          % element 3 = status (updated)
                          lists:nth(4, T),   % element 4 = airport
                          none,              % element 5 = pending (updated)
                          {TripId, Origin},  % element 6 = trip (updated)
                          lists:nth(7, T)]); % element 7 = center
                _ ->
                    io:format("Taxi ~p: no pending offer ~p~n",
                              [lists:nth(1, T), TripId]), % element 1 = id
                    loop(T)
            end;

        %% Operator rejects the pending offer.
        {reject, TripId} ->
            case lists:nth(5, T) of   % element 5 = pending
                {TripId, _Origin} ->
                    log_send(io_lib:format("reject trip ~p to center", [TripId])),
                    lists:nth(7, T) ! {trip_response, reject, TripId,
                                       lists:nth(1, T), self()}, % element 7 = center, element 1 = id
                    %% rebuild, only changing element 5 = pending
                    loop([lists:nth(1, T),   % element 1 = id
                          lists:nth(2, T),   % element 2 = loc
                          lists:nth(3, T),   % element 3 = status
                          lists:nth(4, T),   % element 4 = airport
                          none,              % element 5 = pending (updated)
                          lists:nth(6, T),   % element 6 = trip
                          lists:nth(7, T)]); % element 7 = center
                _ ->
                    io:format("Taxi ~p: no pending offer ~p~n",
                              [lists:nth(1, T), TripId]), % element 1 = id
                    loop(T)
            end;

        %% Center withdrew an offer we never accepted (the passenger cancelled
        %% while the offer was still pending). Just drop element 5 = pending.
        {offer_cancelled, TripId} ->
            case lists:nth(5, T) of   % element 5 = pending
                {TripId, _O} ->
                    log_recv(io_lib:format("offer ~p withdrawn - clearing pending",
                                           [TripId])),
                    %% rebuild, only changing element 5 = pending
                    loop([lists:nth(1, T),   % element 1 = id
                          lists:nth(2, T),   % element 2 = loc
                          lists:nth(3, T),   % element 3 = status
                          lists:nth(4, T),   % element 4 = airport
                          none,              % element 5 = pending (cleared)
                          lists:nth(6, T),   % element 6 = trip
                          lists:nth(7, T)]); % element 7 = center
                _ -> loop(T)
            end;

        %% Center cancelled a reserved (not yet started) trip.
        {trip_cancelled, TripId} ->
            case lists:nth(6, T) of   % element 6 = trip
                {TripId, _O} ->
                    log_recv(io_lib:format("trip ~p cancelled - now available",
                                           [TripId])),
                    %% rebuild, changing element 3 = status and element 6 = trip
                    loop([lists:nth(1, T),   % element 1 = id
                          lists:nth(2, T),   % element 2 = loc
                          available,         % element 3 = status (updated)
                          lists:nth(4, T),   % element 4 = airport
                          lists:nth(5, T),   % element 5 = pending
                          none,              % element 6 = trip (updated)
                          lists:nth(7, T)]); % element 7 = center
                _ -> loop(T)
            end;

        {update_loc, NewLoc} ->
            log_recv(io_lib:format("update location to ~p", [NewLoc])),
            %% rebuild, only changing element 2 = loc
            loop([lists:nth(1, T),   % element 1 = id
                  NewLoc,            % element 2 = loc (updated)
                  lists:nth(3, T),   % element 3 = status
                  lists:nth(4, T),   % element 4 = airport
                  lists:nth(5, T),   % element 5 = pending
                  lists:nth(6, T),   % element 6 = trip
                  lists:nth(7, T)]); % element 7 = center

        %% service_started/1
        {start, From, Ref} ->
            case lists:nth(6, T) of   % element 6 = trip
                {TripId, Origin} ->
                    log_recv("service_started command"),
                    log_send(io_lib:format("service_started trip ~p to center",
                                           [TripId])),
                    lists:nth(7, T) ! {service_started,
                                       lists:nth(1, T), TripId}, % element 7 = center, element 1 = id
                    From ! {result, Ref, ok},
                    %% rebuild, changing element 2 = loc and element 3 = status
                    loop([lists:nth(1, T),   % element 1 = id
                          Origin,            % element 2 = loc (updated)
                          occupied,          % element 3 = status (updated)
                          lists:nth(4, T),   % element 4 = airport
                          lists:nth(5, T),   % element 5 = pending
                          lists:nth(6, T),   % element 6 = trip
                          lists:nth(7, T)]); % element 7 = center
                none ->
                    From ! {result, Ref, {error, no_trip}},
                    loop(T)
            end;

        %% service_completed/1
        {complete, From, Ref} ->
            case {lists:nth(3, T), lists:nth(6, T)} of % element 3 = status, element 6 = trip
                {occupied, {TripId, _O}} ->
                    log_recv("service_completed command"),
                    log_send(io_lib:format("service_completed trip ~p to center",
                                           [TripId])),
                    lists:nth(7, T) ! {service_completed,
                                       lists:nth(1, T), TripId}, % element 7 = center, element 1 = id
                    From ! {result, Ref, ok},
                    %% rebuild, changing element 2 = loc, element 3 = status, element 6 = trip
                    loop([lists:nth(1, T),   % element 1 = id
                          lists:nth(4, T),   % element 2 = loc (updated to airport, which is element 4)
                          available,         % element 3 = status (updated)
                          lists:nth(4, T),   % element 4 = airport
                          lists:nth(5, T),   % element 5 = pending
                          none,              % element 6 = trip (updated)
                          lists:nth(7, T)]); % element 7 = center
                _ ->
                    From ! {result, Ref, {error, not_in_service}},
                    loop(T)
            end;

        {consult, From, Ref} ->
            log_recv("consult request"),
            From ! {taxi_info, Ref,
                    lists:nth(1, T),   % element 1 = id
                    lists:nth(3, T),   % element 3 = status
                    lists:nth(2, T)},  % element 2 = loc
            loop(T);

        %% remove_taxi/1 (only if available)
        {remove, From, Ref} ->
            case lists:nth(3, T) of   % element 3 = status
                available ->
                    log_recv("remove request - available, terminating"),
                    From ! {result, Ref, ok};
                    %% returning stops the process; center's monitor drops it.
                occupied ->
                    log_recv("remove request - occupied, refusing"),
                    From ! {result, Ref, {error, occupied}},
                    loop(T)
            end;

        %% The center went down: stop ourselves. (lists:nth/2 is not allowed in
        %% a guard, so we compare against element 7 = center in the body.)
        {'DOWN', _Ref, process, DownPid, _Reason} ->
            case DownPid =:= lists:nth(7, T) of   % element 7 = center
                true ->
                    io:format("Taxi ~p: center is down, stopping.~n",
                              [lists:nth(1, T)]); % element 1 = id
                    %% returning stops the taxi process.
                false ->
                    loop(T)
            end;

        Other ->
            io:format("Taxi ~p: ignoring ~p~n", [lists:nth(1, T), Other]), % element 1 = id
            loop(T)
    end.

log_send(Msg) -> io:format("Sends: ~s~n", [Msg]).
log_recv(Msg) -> io:format("Receives: ~s~n", [Msg]).
