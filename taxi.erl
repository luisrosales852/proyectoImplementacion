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
%%%   Each registered taxi is an independent process that monitors its own
%%%   status (available / occupied) and location, answers status queries
%%%   from the dispatch center, receives trip offers/assignments and runs
%%%   the transport service.
%%%
%%% PROCESS MODEL
%%%   * register_taxi/2 spawns ONE process and registers it locally
%%%     (register/2) and cluster-wide (global:register_name/2) under the
%%%     unique TaxiId atom, then announces itself to the center.
%%%   * The taxi process DOES NOT trap exits. The center links to it, so:
%%%       - if the center dies abnormally, the link kills this taxi too
%%%         (automatic teardown required by the spec);
%%%       - if this taxi dies (e.g. remove_taxi), the center (which traps
%%%         exits) just drops its entry.
%%%   * The center keeps only {TaxiId, Pid}; the live status/location is
%%%     held HERE and reported on demand via {get_state, From, Ref}.
%%%
%%% NOTE ON accept_trip/reject_trip
%%%   These take only a TripId (per spec). They message the CENTER directly
%%%   ({trip_response, ...}); the center already knows which taxi it offered
%%%   that TripId to, so the TaxiId is not needed in the call. On acceptance
%%%   the center sends {assign, TripId, Origin} back to this taxi process,
%%%   which is what actually turns the taxi `occupied`.
%%%=====================================================================
-module(taxi).

%% ---- Public API (run in the CALLER's process) ----
-export([register_taxi/2, current_location/2,
         accept_trip/1, reject_trip/1,
         service_started/1, service_completed/1,
         remove_taxi/1, consult_taxi/1]).

%% ---- Spawned entry point ----
-export([init/3]).

-define(CALL_TIMEOUT, 10000).

%% Per-taxi state.
-record(taxi, {id,                       % unique atom identifier
               loc,                       % {X,Y} current location
               status = available,        % available | occupied
               airport,                   % {X,Y} cached at registration
               trip_origin = undefined,   % pickup point of the current trip
               trip_id     = undefined,   % id of the current trip
               center}).                  % center Pid (for notifications)

%%%=====================================================================
%%% PUBLIC API
%%%=====================================================================

%% Register a taxi at the central with an initial location {X,Y}.
register_taxi(TaxiId, InitialLocation) ->
    case resolve(TaxiId) of
        Pid when is_pid(Pid) ->
            io:format("Taxi ~p is already registered.~n", [TaxiId]),
            {error, already_registered};
        undefined ->
            case resolve(center) of
                undefined ->
                    io:format("No dispatch center reachable; cannot register "
                              "taxi ~p.~n", [TaxiId]),
                    {error, no_center};
                _Center ->
                    Parent = self(),
                    Pid = spawn(?MODULE, init,
                                [TaxiId, InitialLocation, Parent]),
                    receive
                        {taxi_ready, Pid} ->
                            io:format("Taxi ~p registered at ~p (node ~p).~n",
                                      [TaxiId, InitialLocation, node()]),
                            {ok, Pid};
                        {taxi_error, Reason} ->
                            {error, Reason}
                    after ?CALL_TIMEOUT ->
                        {error, timeout}
                    end
            end
    end.

%% Update a taxi's current location.
current_location(TaxiId, Location) ->
    cast(TaxiId, fun(Pid) ->
        log_send(io_lib:format("update location of ~p to ~p", [TaxiId, Location])),
        Pid ! {update_location, Location}
    end).

%% Accept a trip offer identified by TripId (answers the center).
accept_trip(TripId) ->
    respond_to_offer(accept, TripId).

%% Reject a trip offer identified by TripId (the center tries the next taxi).
reject_trip(TripId) ->
    respond_to_offer(reject, TripId).

%% Notify the center that the taxi has started its transport service.
service_started(TaxiId) ->
    call_taxi(TaxiId, {start_service, self(), make_ref()}, service_result).

%% Notify the center that the taxi has completed its transport service.
service_completed(TaxiId) ->
    call_taxi(TaxiId, {complete_service, self(), make_ref()}, service_result).

%% Remove (terminate) a taxi. Only allowed when the taxi is available.
remove_taxi(TaxiId) ->
    call_taxi(TaxiId, {remove, self(), make_ref()}, remove_result).

%% Check the status and location of a taxi.
consult_taxi(TaxiId) ->
    case resolve(TaxiId) of
        undefined ->
            io:format("Taxi ~p is not registered/reachable.~n", [TaxiId]),
            {error, not_found};
        Pid ->
            Ref = make_ref(),
            log_send(io_lib:format("consult taxi ~p", [TaxiId])),
            Pid ! {consult, self(), Ref},
            receive
                {taxi_info, Ref, Id, Status, Loc} ->
                    io:format("~nTaxi ~p | status: ~p | location: ~p~n",
                              [Id, Status, Loc]),
                    {ok, Status, Loc}
            after ?CALL_TIMEOUT ->
                {error, timeout}
            end
    end.

%%%=====================================================================
%%% PROCESS INITIALISATION + MAIN LOOP
%%%=====================================================================

%% Runs inside the freshly spawned taxi process.
init(TaxiId, Loc, Parent) ->
    case global:register_name(TaxiId, self()) of
        yes ->
            catch register(TaxiId, self()),
            Center = resolve(center),
            log_send(io_lib:format("register taxi ~p at ~p", [TaxiId, Loc])),
            Center ! {register_taxi, TaxiId, self(), Loc},
            receive
                {registered, CenterPid, Airport} ->
                    log_recv(io_lib:format("registration ack from center, "
                                           "airport at ~p", [Airport])),
                    Parent ! {taxi_ready, self()},
                    loop(#taxi{id = TaxiId, loc = Loc, status = available,
                               airport = Airport, center = CenterPid});
                {register_error, Reason} ->
                    catch global:unregister_name(TaxiId),
                    Parent ! {taxi_error, Reason}
            after ?CALL_TIMEOUT ->
                catch global:unregister_name(TaxiId),
                Parent ! {taxi_error, no_center_ack}
            end;
        no ->
            Parent ! {taxi_error, name_taken}
    end.

loop(T) ->
    receive
        %% ---- Center asks for live status + location -------------------
        {get_state, From, Ref} ->
            log_recv("status query from center"),
            log_send(io_lib:format("status ~p at ~p to center",
                                   [T#taxi.status, T#taxi.loc])),
            From ! {taxi_state, Ref, T#taxi.id, T#taxi.status, T#taxi.loc},
            loop(T);

        %% ---- consult_taxi/1 -------------------------------------------
        {consult, From, Ref} ->
            log_recv("consult request"),
            From ! {taxi_info, Ref, T#taxi.id, T#taxi.status, T#taxi.loc},
            loop(T);

        %% ---- Trip OFFER from the center (informational) ---------------
        {offer, TripId, Name, Origin} ->
            log_recv(io_lib:format("trip offer ~p from passenger ~p (origin ~p)",
                                   [TripId, Name, Origin])),
            io:format("   -> To respond: taxi:accept_trip(~p)  OR  "
                      "taxi:reject_trip(~p)~n", [TripId, TripId]),
            loop(T);

        %% ---- Center commits the assignment (taxi becomes occupied) ----
        {assign, TripId, Origin} ->
            log_recv(io_lib:format("assignment of trip ~p (origin ~p) - "
                                   "now OCCUPIED", [TripId, Origin])),
            loop(T#taxi{status = occupied, trip_origin = Origin,
                        trip_id = TripId});

        %% ---- Center cancels the (not yet started) trip ----------------
        {trip_cancelled, TripId} ->
            case T#taxi.trip_id of
                TripId ->
                    log_recv(io_lib:format("trip ~p cancelled - now AVAILABLE",
                                           [TripId])),
                    loop(T#taxi{status = available, trip_origin = undefined,
                                trip_id = undefined});
                _ ->
                    loop(T)
            end;

        %% ---- Location update ------------------------------------------
        {update_location, NewLoc} ->
            log_recv(io_lib:format("update location to ~p", [NewLoc])),
            loop(T#taxi{loc = NewLoc});

        %% ---- service_started/1 ----------------------------------------
        {start_service, From, Ref} ->
            case T#taxi.trip_id of
                undefined ->
                    From ! {service_result, Ref, {error, no_trip}},
                    loop(T);
                TripId ->
                    Origin = T#taxi.trip_origin,
                    log_recv("service_started command"),
                    log_send(io_lib:format("service_started for trip ~p to center",
                                           [TripId])),
                    T#taxi.center ! {service_started, T#taxi.id, TripId},
                    From ! {service_result, Ref, ok},
                    %% Drive to the pickup point: occupied at the trip origin.
                    loop(T#taxi{status = occupied, loc = Origin})
            end;

        %% ---- service_completed/1 --------------------------------------
        {complete_service, From, Ref} ->
            case {T#taxi.status, T#taxi.trip_id} of
                {occupied, TripId} when TripId =/= undefined ->
                    log_recv("service_completed command"),
                    log_send(io_lib:format("service_completed for trip ~p to center",
                                           [TripId])),
                    T#taxi.center ! {service_completed, T#taxi.id, TripId},
                    From ! {service_result, Ref, ok},
                    %% Dropped the passenger at the airport: available there.
                    loop(T#taxi{status = available, loc = T#taxi.airport,
                                trip_origin = undefined, trip_id = undefined});
                _ ->
                    From ! {service_result, Ref, {error, not_in_service}},
                    loop(T)
            end;

        %% ---- remove_taxi/1 (only if available) ------------------------
        {remove, From, Ref} ->
            case T#taxi.status of
                available ->
                    log_recv("remove request - taxi available, terminating"),
                    From ! {remove_result, Ref, ok},
                    %% Exit normally; the center (trapping exits) drops the entry.
                    exit(normal);
                occupied ->
                    log_recv("remove request - taxi occupied, refusing"),
                    From ! {remove_result, Ref, {error, occupied}},
                    loop(T)
            end;

        Other ->
            io:format("Taxi ~p: ignoring unexpected message ~p~n",
                      [T#taxi.id, Other]),
            loop(T)
    end.

%%%=====================================================================
%%% INTERNAL HELPERS
%%%=====================================================================

%% accept/reject share the same shape: notify the center, which matches the
%% response to the in-flight offer for that TripId.
respond_to_offer(Decision, TripId) ->
    case resolve(center) of
        undefined ->
            io:format("No dispatch center reachable.~n"),
            {error, no_center};
        Center ->
            log_send(io_lib:format("~p trip ~p", [Decision, TripId])),
            Center ! {trip_response, Decision, TripId, self()},
            ok
    end.

%% Fire-and-forget command to a taxi process (resolves the TaxiId).
cast(TaxiId, Fun) ->
    case resolve(TaxiId) of
        undefined ->
            io:format("Taxi ~p is not registered/reachable.~n", [TaxiId]),
            {error, not_found};
        Pid ->
            Fun(Pid),
            ok
    end.

%% Synchronous command to a taxi process expecting {Tag, Ref, Result}.
call_taxi(TaxiId, {_Cmd, _From, Ref} = Msg, Tag) ->
    case resolve(TaxiId) of
        undefined ->
            io:format("Taxi ~p is not registered/reachable.~n", [TaxiId]),
            {error, not_found};
        Pid ->
            log_send(io_lib:format("~p to taxi ~p", [element(1, Msg), TaxiId])),
            Pid ! Msg,
            receive
                {Tag, Ref, Result} ->
                    report(element(1, Msg), TaxiId, Result),
                    Result
            after ?CALL_TIMEOUT ->
                {error, timeout}
            end
    end.

%% Friendly console feedback for the synchronous taxi commands.
report(start_service, TaxiId, ok) ->
    io:format("Taxi ~p started its service.~n", [TaxiId]);
report(complete_service, TaxiId, ok) ->
    io:format("Taxi ~p completed its service (now available at airport).~n",
              [TaxiId]);
report(remove, TaxiId, ok) ->
    io:format("Taxi ~p removed.~n", [TaxiId]);
report(_Cmd, TaxiId, {error, Reason}) ->
    io:format("Taxi ~p command failed: ~p~n", [TaxiId, Reason]);
report(_Cmd, _TaxiId, _Other) ->
    ok.

%% Resolve a registered name to a Pid: local table first, then global.
resolve(Name) ->
    case whereis(Name) of
        undefined -> global:whereis_name(Name);
        Pid       -> Pid
    end.

log_send(Msg) -> io:format("Sends: ~s~n", [Msg]).
log_recv(Msg) -> io:format("Receives: ~s~n", [Msg]).
