%%%=====================================================================
%%% TC2037 - Implementation of Computational Methods
%%% Activity 6.2 - Distributed Programming in Erlang
%%% Distributed Taxi Rental System for Travel to an Airport
%%%
%%% MODULE: taxi
%%%
%%% TEAM MEMBERS (fill in before submitting):
%%%   Author: [Full Name - A01234567]
%%%   Author: [Full Name - A01234567]
%%%   Author: [Full Name - A01234567]
%%%
%%% PURPOSE
%%%   Each registered taxi is its own process that holds its status
%%%   (available / occupied) and location, answers the center's status
%%%   queries, receives trip offers and runs the transport service.
%%%
%%% PROCESS MODEL  (plain, PID-based)
%%%   * register_taxi/3 spawns the taxi process and RETURNS its PID. All
%%%     taxi commands are sent to that PID (no registered name is used).
%%%   * The taxi monitors the center (erlang:monitor): if the center goes
%%%     DOWN (closed or crashed) the taxi stops itself automatically.
%%%   * The center monitors the taxi, so if the taxi stops, the center just
%%%     drops its entry.
%%%   * accept_trip/reject_trip are sent to the taxi PID; on accept the taxi
%%%     marks itself occupied, remembers the trip and notifies the center.
%%%=====================================================================
-module(taxi).

%% Public API (run in the CALLER's process, using the taxi's PID).
-export([register_taxi/3, current_location/2,
         accept_trip/2, reject_trip/2,
         service_started/1, service_completed/1,
         remove_taxi/1, consult_taxi/1]).

%% Spawned entry point.
-export([init/4]).

-define(CALL_TIMEOUT, 10000).

-record(taxi, {id,                    % atom label
               loc,                    % {X,Y}
               status  = available,    % available | occupied
               airport,                % {X,Y} cached at registration
               pending = none,         % {TripId, Origin} of an outstanding offer
               trip    = none,         % {TripId, Origin} of the current trip
               center}).               % center PID

%%%=====================================================================
%%% PUBLIC API
%%%=====================================================================

%% Register a taxi at the center. Returns the taxi's PID (use it for the
%% other commands). CenterPid comes from open_center/1 or center:find/1.
register_taxi(TaxiId, InitialLocation, CenterPid) ->
    Parent = self(),
    Pid = spawn(?MODULE, init, [TaxiId, InitialLocation, CenterPid, Parent]),
    receive
        {taxi_ready, Pid} ->
            io:format("Taxi ~p registered at ~p, PID ~p (node ~p).~n",
                      [TaxiId, InitialLocation, Pid, node()]),
            Pid;
        {taxi_error, Reason} ->
            io:format("Taxi ~p not registered: ~p~n", [TaxiId, Reason]),
            {error, Reason}
    after ?CALL_TIMEOUT ->
        {error, timeout}
    end.

%% Update a taxi's location.
current_location(TaxiPid, Location) ->
    log_send(io_lib:format("update location to ~p", [Location])),
    TaxiPid ! {update_loc, Location},
    ok.

%% Accept / reject the trip offer TripId (answers the center via the taxi).
accept_trip(TaxiPid, TripId) ->
    log_send(io_lib:format("accept trip ~p", [TripId])),
    TaxiPid ! {accept, TripId},
    ok.

reject_trip(TaxiPid, TripId) ->
    log_send(io_lib:format("reject trip ~p", [TripId])),
    TaxiPid ! {reject, TripId},
    ok.

%% Notify the center that the service started (taxi occupied at the origin).
service_started(TaxiPid) ->
    sync(TaxiPid, start, "Taxi started its service.").

%% Notify the center that the service finished (taxi available at airport).
service_completed(TaxiPid) ->
    sync(TaxiPid, complete, "Taxi completed its service (available at airport).").

%% Remove the taxi (only allowed when available).
remove_taxi(TaxiPid) ->
    sync(TaxiPid, remove, "Taxi removed.").

%% Check a taxi's status and location.
consult_taxi(TaxiPid) ->
    Ref = make_ref(),
    log_send("consult taxi"),
    TaxiPid ! {consult, self(), Ref},
    receive
        {taxi_info, Ref, Id, Status, Loc} ->
            io:format("~nTaxi ~p | status: ~p | location: ~p~n", [Id, Status, Loc]),
            {ok, Status, Loc}
    after ?CALL_TIMEOUT -> {error, timeout}
    end.

%% Shared synchronous command (start | complete | remove).
sync(TaxiPid, Cmd, OkMsg) ->
    Ref = make_ref(),
    log_send(io_lib:format("~p command to taxi", [Cmd])),
    TaxiPid ! {Cmd, self(), Ref},
    receive
        {result, Ref, ok}             -> io:format("~s~n", [OkMsg]), ok;
        {result, Ref, {error, Why}}   -> io:format("Command failed: ~p~n", [Why]),
                                         {error, Why}
    after ?CALL_TIMEOUT -> {error, timeout}
    end.

%%%=====================================================================
%%% PROCESS LOOP
%%%=====================================================================

init(TaxiId, Loc, CenterPid, Parent) ->
    erlang:monitor(process, CenterPid),     % center dies -> we stop ourselves
    log_send(io_lib:format("register taxi ~p at ~p", [TaxiId, Loc])),
    CenterPid ! {register_taxi, TaxiId, self(), Loc},
    receive
        {registered, Airport} ->
            log_recv(io_lib:format("registration ack, airport at ~p", [Airport])),
            Parent ! {taxi_ready, self()},
            loop(#taxi{id = TaxiId, loc = Loc, status = available,
                       airport = Airport, center = CenterPid});
        {register_error, Reason} ->
            Parent ! {taxi_error, Reason}
    after ?CALL_TIMEOUT ->
        Parent ! {taxi_error, no_center_ack}
    end.

loop(T) ->
    receive
        %% Center asks for live status + location.
        {get_state, From, Ref} ->
            log_recv("status query from center"),
            log_send(io_lib:format("status ~p at ~p to center",
                                   [T#taxi.status, T#taxi.loc])),
            From ! {taxi_state, Ref, T#taxi.id, T#taxi.status, T#taxi.loc},
            loop(T);

        %% Trip offer (informational; remember it so accept/reject can use it).
        {offer, TripId, Name, Origin} ->
            log_recv(io_lib:format("trip offer ~p from ~p (origin ~p)",
                                   [TripId, Name, Origin])),
            io:format("   -> respond: taxi:accept_trip(self_pid,~p) OR "
                      "taxi:reject_trip(self_pid,~p)~n", [TripId, TripId]),
            loop(T#taxi{pending = {TripId, Origin}});

        %% Operator accepts the pending offer.
        {accept, TripId} ->
            case T#taxi.pending of
                {TripId, Origin} ->
                    log_send(io_lib:format("accept trip ~p to center", [TripId])),
                    T#taxi.center ! {trip_response, accept, TripId, T#taxi.id, self()},
                    loop(T#taxi{status = occupied, pending = none,
                                trip = {TripId, Origin}});
                _ ->
                    io:format("Taxi ~p: no pending offer ~p~n", [T#taxi.id, TripId]),
                    loop(T)
            end;

        %% Operator rejects the pending offer.
        {reject, TripId} ->
            case T#taxi.pending of
                {TripId, _Origin} ->
                    log_send(io_lib:format("reject trip ~p to center", [TripId])),
                    T#taxi.center ! {trip_response, reject, TripId, T#taxi.id, self()},
                    loop(T#taxi{pending = none});
                _ ->
                    io:format("Taxi ~p: no pending offer ~p~n", [T#taxi.id, TripId]),
                    loop(T)
            end;

        %% Center cancelled a reserved (not yet started) trip.
        {trip_cancelled, TripId} ->
            case T#taxi.trip of
                {TripId, _O} ->
                    log_recv(io_lib:format("trip ~p cancelled - now available",
                                           [TripId])),
                    loop(T#taxi{status = available, trip = none});
                _ -> loop(T)
            end;

        {update_loc, NewLoc} ->
            log_recv(io_lib:format("update location to ~p", [NewLoc])),
            loop(T#taxi{loc = NewLoc});

        %% service_started/1
        {start, From, Ref} ->
            case T#taxi.trip of
                {TripId, Origin} ->
                    log_recv("service_started command"),
                    log_send(io_lib:format("service_started trip ~p to center",
                                           [TripId])),
                    T#taxi.center ! {service_started, T#taxi.id, TripId},
                    From ! {result, Ref, ok},
                    loop(T#taxi{status = occupied, loc = Origin});
                none ->
                    From ! {result, Ref, {error, no_trip}},
                    loop(T)
            end;

        %% service_completed/1
        {complete, From, Ref} ->
            case {T#taxi.status, T#taxi.trip} of
                {occupied, {TripId, _O}} ->
                    log_recv("service_completed command"),
                    log_send(io_lib:format("service_completed trip ~p to center",
                                           [TripId])),
                    T#taxi.center ! {service_completed, T#taxi.id, TripId},
                    From ! {result, Ref, ok},
                    loop(T#taxi{status = available, loc = T#taxi.airport,
                                trip = none});
                _ ->
                    From ! {result, Ref, {error, not_in_service}},
                    loop(T)
            end;

        {consult, From, Ref} ->
            log_recv("consult request"),
            From ! {taxi_info, Ref, T#taxi.id, T#taxi.status, T#taxi.loc},
            loop(T);

        %% remove_taxi/1 (only if available)
        {remove, From, Ref} ->
            case T#taxi.status of
                available ->
                    log_recv("remove request - available, terminating"),
                    From ! {result, Ref, ok};
                    %% returning stops the process; center's monitor drops it.
                occupied ->
                    log_recv("remove request - occupied, refusing"),
                    From ! {result, Ref, {error, occupied}},
                    loop(T)
            end;

        %% The center went down: stop ourselves.
        {'DOWN', _Ref, process, CenterPid, _Reason}
          when CenterPid =:= T#taxi.center ->
            io:format("Taxi ~p: center is down, stopping.~n", [T#taxi.id]);
            %% returning stops the taxi process.

        Other ->
            io:format("Taxi ~p: ignoring ~p~n", [T#taxi.id, Other]),
            loop(T)
    end.

log_send(Msg) -> io:format("Sends: ~s~n", [Msg]).
log_recv(Msg) -> io:format("Receives: ~s~n", [Msg]).
