%%%=====================================================================
%%% TC2037 - Implementation of Computational Methods
%%% Activity 6.2 - Distributed Programming in Erlang
%%% Distributed Taxi Rental System for Travel to an Airport
%%%
%%% MODULE: center  (Taxi Dispatch Center)
%%%
%%% TEAM MEMBERS (fill in before submitting):
%%%   Author: [Full Name - A01234567]
%%%   Author: [Full Name - A01234567]
%%%   Author: [Full Name - A01234567]
%%%
%%% PURPOSE
%%%   The Taxi Dispatch Center is a single process that centralizes all
%%%   information of the system: the list of registered (active) taxis,
%%%   the list of current passengers, the list of trips in progress and
%%%   the history of completed trips. It assigns the nearest AVAILABLE
%%%   taxi (Euclidean distance) to each traveler's request, generates a
%%%   unique trip id per assignment and keeps a trip counter.
%%%
%%% PROCESS MODEL
%%%   * open_center/1 spawns ONE process and registers it locally
%%%     (register/2) AND cluster-wide (global:register_name/2) with the
%%%     name `center`, so travelers and taxis never need its PID or node.
%%%   * The center process TRAPS EXITS and LINKS to every taxi process.
%%%       - A taxi process dying  -> center receives {'EXIT',Pid,_} and
%%%         only removes that taxi's entry (the center survives).
%%%       - The center dying with a NON-normal reason -> the exit signal
%%%         propagates over the links and kills every (non-trapping) taxi
%%%         automatically (spec: "if the dispatch process terminates, all
%%%         taxi processes must also terminate").
%%%   * Per spec, the center stores ONLY {TaxiId, Pid} for each taxi; the
%%%     live status/location lives inside each taxi process, so the center
%%%     QUERIES the taxis (get_state) whenever it needs to sort by distance
%%%     or to display taxi_list/0.
%%%
%%% COMMUNICATION PROTOCOL (every exchange is traced with Sends:/Receives:)
%%%   traveler  -> center : {request_taxi, Name, Origin, PassPid}
%%%                         {cancel, Name, From}
%%%   center    -> taxi   : {get_state, Self, Ref}            (status query)
%%%                         {offer, TripId, Name, Origin}     (trip offer)
%%%                         {assign, TripId, Origin}          (commit)
%%%                         {registered, Self, Airport}       (ack register)
%%%                         {trip_cancelled, TripId}          (free taxi)
%%%   taxi      -> center : {register_taxi, TaxiId, Pid, Loc}
%%%                         {taxi_state, Ref, TaxiId, Status, Loc}
%%%                         {trip_response, accept|reject, TripId, Pid}
%%%                         {service_started, TaxiId, TripId}
%%%                         {service_completed, TaxiId, TripId}
%%%   center    -> trav   : {taxi_assigned, TaxiId, TripId} | {no_taxi}
%%%                         {rejected, duplicate} | {cancelled}
%%%                         {trip_completed, TripId}
%%%   API calls -> center : {taxi_list,From} {travelers_list,From}
%%%                         {completed_trips,From} {close,From}
%%%=====================================================================
-module(center).

%% ---- Public API (interface functions, run in the CALLER's process) ----
-export([open_center/1, close_center/0,
         taxi_list/0, travelers_list/0, completed_trips/0]).

%% ---- Spawned entry point (must be exported for spawn/1 by name) ----
-export([init/2]).

%% Reply timeout (ms) for the synchronous API helpers.
-define(CALL_TIMEOUT, 10000).
%% How long the center waits for a taxi's accept/reject before treating
%% the silence as a rejection and moving on to the next-closest taxi.
-define(OFFER_TIMEOUT, 60000).
%% How long the center waits for the taxis' status replies during a query.
-define(QUERY_TIMEOUT, 3000).

%% Center state. `taxis` keeps ONLY {TaxiId, Pid} as required by the spec.
-record(state, {airport,            % {X,Y} airport coordinates
                taxis      = [],     % [{TaxiId, Pid}]
                passengers = [],     % [{Name, PassPid, Origin}] current travelers
                active     = [],     % [{TripId,TaxiId,TaxiPid,Name,PassPid,Origin,assigned|in_service}]
                completed  = [],     % [{TripId,TaxiId,Name,Origin}] history (newest first)
                counter    = 0}).    % trip counter, ++ on each committed assignment

%%%=====================================================================
%%% PUBLIC API
%%%=====================================================================

%% Create the Taxi Dispatch Center process at the given airport location.
%% Idempotent across the cluster: refuses if a center is already open.
open_center(AirportLocation) ->
    case global:whereis_name(center) of
        undefined ->
            Parent = self(),
            Pid = spawn(?MODULE, init, [AirportLocation, Parent]),
            receive
                {center_ready, Pid} ->
                    io:format("== Taxi Dispatch Center OPEN at airport ~p "
                              "(node ~p) ==~n", [AirportLocation, node()]),
                    {ok, Pid};
                {center_error, Reason} ->
                    {error, Reason}
            after ?CALL_TIMEOUT ->
                {error, timeout}
            end;
        _Pid ->
            io:format("Dispatch center is already open.~n"),
            {error, already_open}
    end.

%% Terminate the dispatch center (and, via the links, every taxi).
close_center() ->
    call_center({close, self()}, close_result).

%% Display the list of active taxis with all relevant information.
%% Re-queries every taxi process for its live status and location.
taxi_list() ->
    case call_center({taxi_list, self()}, taxi_list_result) of
        {error, _} = E -> E;
        States ->
            io:format("~n--- ACTIVE TAXIS (~p) ---~n", [length(States)]),
            lists:foreach(
              fun({TaxiId, Status, Loc}) ->
                  io:format("  Taxi ~p | status: ~p | location: ~p~n",
                            [TaxiId, Status, Loc])
              end, States),
            io:format("-------------------------~n"),
            ok
    end.

%% Display the list of current passengers (travelers being served).
travelers_list() ->
    case call_center({travelers_list, self()}, travelers_list_result) of
        {error, _} = E -> E;
        {Passengers, Active} ->
            io:format("~n--- CURRENT PASSENGERS (~p) ---~n",
                      [length(Passengers)]),
            lists:foreach(
              fun({Name, _Pid, Origin}) ->
                  Status = passenger_status(Name, Active),
                  io:format("  Passenger ~p | origin: ~p | ~s~n",
                            [Name, Origin, Status])
              end, Passengers),
            io:format("-------------------------------~n"),
            ok
    end.

%% Display the history of completed trips and the trip counter.
completed_trips() ->
    case call_center({completed_trips, self()}, completed_trips_result) of
        {error, _} = E -> E;
        {Counter, Completed} ->
            io:format("~n--- COMPLETED TRIPS (total assignments: ~p, "
                      "completed: ~p) ---~n", [Counter, length(Completed)]),
            lists:foreach(
              fun({TripId, TaxiId, Name, Origin}) ->
                  io:format("  Trip ~p | taxi: ~p | passenger: ~p | "
                            "origin: ~p~n", [TripId, TaxiId, Name, Origin])
              end, lists:reverse(Completed)),
            io:format("-----------------------------------------------~n"),
            ok
    end.

%%%=====================================================================
%%% PROCESS INITIALISATION + MAIN LOOP
%%%=====================================================================

%% Runs inside the freshly spawned center process.
init(Airport, Parent) ->
    process_flag(trap_exit, true),          % survive taxi deaths; own the links
    case global:register_name(center, self()) of
        yes ->
            catch register(center, self()),  % local convenience on this node
            Parent ! {center_ready, self()},
            loop(#state{airport = Airport});
        no ->
            Parent ! {center_error, already_open}
    end.

loop(State) ->
    receive
        %% ---- A traveler requests a taxi -------------------------------
        {request_taxi, Name, Origin, PassPid} ->
            log_recv(io_lib:format("taxi request from ~p", [Name])),
            loop(handle_request(Name, Origin, PassPid, State));

        %% ---- A traveler cancels a request that has not started --------
        {cancel, Name, From} ->
            log_recv(io_lib:format("cancel request from ~p", [Name])),
            loop(handle_cancel(Name, From, State));

        %% ---- A taxi registers itself ----------------------------------
        {register_taxi, TaxiId, TaxiPid, Loc} ->
            log_recv(io_lib:format("register taxi ~p at ~p", [TaxiId, Loc])),
            loop(handle_register(TaxiId, TaxiPid, Loc, State));

        %% ---- A taxi notifies the start of a service -------------------
        {service_started, TaxiId, TripId} ->
            log_recv(io_lib:format("service started by taxi ~p (trip ~p)",
                                   [TaxiId, TripId])),
            loop(State#state{active =
                     set_trip_state(TripId, in_service, State#state.active)});

        %% ---- A taxi notifies the completion of a service --------------
        {service_completed, TaxiId, TripId} ->
            log_recv(io_lib:format("service completed by taxi ~p (trip ~p)",
                                   [TaxiId, TripId])),
            loop(handle_completed(TaxiId, TripId, State));

        %% ---- A linked taxi (or passenger) process terminated ----------
        {'EXIT', Pid, Reason} ->
            loop(handle_exit(Pid, Reason, State));

        %% ---- center:taxi_list/0 ---------------------------------------
        {taxi_list, From} ->
            From ! {taxi_list_result, query_taxis(State#state.taxis)},
            loop(State);

        %% ---- center:travelers_list/0 ----------------------------------
        {travelers_list, From} ->
            From ! {travelers_list_result,
                    {State#state.passengers, State#state.active}},
            loop(State);

        %% ---- center:completed_trips/0 ---------------------------------
        {completed_trips, From} ->
            From ! {completed_trips_result,
                    {State#state.counter, State#state.completed}},
            loop(State);

        %% ---- center:close_center/0 ------------------------------------
        {close, From} ->
            log_recv("close_center request"),
            From ! {close_result, ok},
            io:format("== Closing dispatch center: terminating ~p taxi(s) ==~n",
                      [length(State#state.taxis)]),
            %% A `normal` exit would NOT kill non-trapping linked taxis, so
            %% kill them explicitly, then die with a non-normal reason so any
            %% linked passenger processes are torn down too.
            [ exit(Pid, kill) || {_Id, Pid} <- State#state.taxis ],
            exit(shutdown);

        %% ---- Defensive: drop unexpected / stale messages --------------
        Other ->
            io:format("Center: ignoring unexpected message ~p~n", [Other]),
            loop(State)
    end.

%%%=====================================================================
%%% REQUEST HANDLING / ASSIGNMENT
%%%=====================================================================

%% Validate uniqueness, register the passenger and run the dispatch.
handle_request(Name, Origin, PassPid, State) ->
    case lists:keymember(Name, 1, State#state.passengers) of
        true ->
            log_send(io_lib:format("reject ~p (duplicate passenger)", [Name])),
            PassPid ! {rejected, duplicate},
            State;
        false ->
            S1 = State#state{passengers =
                     [{Name, PassPid, Origin} | State#state.passengers]},
            dispatch(Name, Origin, PassPid, S1)
    end.

%% Query taxis, keep the available ones sorted by Euclidean distance to the
%% origin, and offer the trip from closest to farthest.
dispatch(Name, Origin, PassPid, State) ->
    States = query_taxis(State#state.taxis),       % [{TaxiId,Status,Loc}]
    Ranked = lists:sort(
               [ {sq_dist(Loc, Origin), TaxiId}
                 || {TaxiId, available, Loc} <- States ]),
    Candidates = [ {TaxiId, taxi_pid(TaxiId, State)}
                   || {_D, TaxiId} <- Ranked ],
    TripId = State#state.counter + 1,              % candidate id (commit on accept)
    offer_loop(Candidates, TripId, Name, Origin, PassPid, State).

%% Offer the trip to each candidate in turn. Handles four outcomes per
%% offer: accept (commit), reject (next), offered-taxi crash (next),
%% timeout (next). Empty list => no taxi available.
offer_loop([], _TripId, Name, _Origin, PassPid, State) ->
    log_send(io_lib:format("no taxi available for ~p", [Name])),
    PassPid ! {no_taxi},
    State#state{passengers = lists:keydelete(Name, 1, State#state.passengers)};
offer_loop([{TaxiId, TaxiPid} | Rest], TripId, Name, Origin, PassPid, State) ->
    log_send(io_lib:format("trip offer ~p to taxi ~p (origin ~p)",
                           [TripId, TaxiId, Origin])),
    TaxiPid ! {offer, TripId, Name, Origin},
    receive
        {trip_response, accept, TripId, _From} ->
            log_recv(io_lib:format("taxi ~p ACCEPTED trip ~p", [TaxiId, TripId])),
            log_send(io_lib:format("assign trip ~p to taxi ~p", [TripId, TaxiId])),
            TaxiPid ! {assign, TripId, Origin},          % taxi -> occupied
            log_send(io_lib:format("taxi ~p assigned to ~p (trip ~p)",
                                   [TaxiId, Name, TripId])),
            PassPid ! {taxi_assigned, TaxiId, TripId},   % answer the traveler
            Trip = {TripId, TaxiId, TaxiPid, Name, PassPid, Origin, assigned},
            State#state{counter = TripId,                % commit the counter
                        active  = [Trip | State#state.active]};
        {trip_response, reject, TripId, _From} ->
            log_recv(io_lib:format("taxi ~p REJECTED trip ~p - trying next",
                                   [TaxiId, TripId])),
            offer_loop(Rest, TripId, Name, Origin, PassPid, State);
        {'EXIT', TaxiPid, _Reason} ->
            log_recv(io_lib:format("taxi ~p died during offer - trying next",
                                   [TaxiId])),
            offer_loop(Rest, TripId, Name, Origin, PassPid,
                       remove_taxi_pid(TaxiPid, State))
    after ?OFFER_TIMEOUT ->
        log_recv(io_lib:format("taxi ~p did not answer offer ~p - trying next",
                               [TaxiId, TripId])),
        offer_loop(Rest, TripId, Name, Origin, PassPid, State)
    end.

%%%=====================================================================
%%% OTHER HANDLERS
%%%=====================================================================

%% Register a new taxi: link to it and remember {TaxiId, Pid}.
handle_register(TaxiId, TaxiPid, _Loc, State) ->
    case lists:keymember(TaxiId, 1, State#state.taxis) of
        true ->
            TaxiPid ! {register_error, already_registered},
            State;
        false ->
            link(TaxiPid),                              % lifecycle coupling
            log_send(io_lib:format("airport ~p to taxi ~p",
                                   [State#state.airport, TaxiId])),
            TaxiPid ! {registered, self(), State#state.airport},
            State#state{taxis = [{TaxiId, TaxiPid} | State#state.taxis]}
    end.

%% A taxi finished a service: archive the trip and complete the passenger.
handle_completed(TaxiId, TripId, State) ->
    case lists:keyfind(TripId, 1, State#state.active) of
        {TripId, TaxiId, _TaxiPid, Name, PassPid, Origin, _St} ->
            log_send(io_lib:format("trip ~p completed -> passenger ~p", [TripId, Name])),
            PassPid ! {trip_completed, TripId},         % complete passenger process
            State#state{
              active     = lists:keydelete(TripId, 1, State#state.active),
              passengers = lists:keydelete(Name, 1, State#state.passengers),
              completed  = [{TripId, TaxiId, Name, Origin} | State#state.completed]};
        _ ->
            State
    end.

%% Cancel a request only if its service has NOT started yet.
handle_cancel(Name, From, State) ->
    case find_trip_by_name(Name, State#state.active) of
        {TripId, _TaxiId, TaxiPid, Name, PassPid, _Origin, assigned} ->
            log_send(io_lib:format("cancel trip ~p (free taxi, notify passenger)",
                                   [TripId])),
            TaxiPid ! {trip_cancelled, TripId},         % taxi -> available
            PassPid ! {cancelled},
            From ! {cancel_result, ok},
            State#state{
              active     = lists:keydelete(TripId, 1, State#state.active),
              passengers = lists:keydelete(Name, 1, State#state.passengers)};
        {_TripId, _TaxiId, _TaxiPid, Name, _PassPid, _Origin, in_service} ->
            From ! {cancel_result, {error, service_already_started}},
            State;
        false ->
            %% Passenger registered but no committed trip (rare): cancel it.
            case lists:keyfind(Name, 1, State#state.passengers) of
                {Name, PassPid, _O} ->
                    PassPid ! {cancelled},
                    From ! {cancel_result, ok},
                    State#state{passengers =
                        lists:keydelete(Name, 1, State#state.passengers)};
                false ->
                    From ! {cancel_result, {error, not_found}},
                    State
            end
    end.

%% A linked process died. Spec: a taxi dying only removes its list entry
%% (the center survives). Passenger deaths are cleaned up too.
handle_exit(Pid, Reason, State) ->
    case lists:keyfind(Pid, 2, State#state.taxis) of
        {TaxiId, Pid} ->
            log_recv(io_lib:format("taxi ~p process terminated (~p) - "
                                   "removing entry", [TaxiId, Reason])),
            State#state{taxis = lists:keydelete(Pid, 2, State#state.taxis)};
        false ->
            case lists:keyfind(Pid, 2, State#state.passengers) of
                {Name, Pid, _O} ->
                    State#state{
                      passengers = lists:keydelete(Pid, 2, State#state.passengers),
                      active     = remove_trip_by_name(Name, State#state.active)};
                false ->
                    State   % unrelated EXIT (e.g. killed-taxi ack during close)
            end
    end.

%%%=====================================================================
%%% TAXI STATUS QUERY (center stores only id+pid, so it asks the taxis)
%%%=====================================================================

%% Ask every taxi for {status, location}. Tolerates non-responders.
query_taxis(Taxis) ->
    Ref = make_ref(),
    [ Pid ! {get_state, self(), Ref} || {_Id, Pid} <- Taxis ],
    log_send(io_lib:format("status query to ~p taxi(s)", [length(Taxis)])),
    collect_states(Ref, length(Taxis), []).

collect_states(_Ref, 0, Acc) -> Acc;
collect_states(Ref, N, Acc) ->
    receive
        {taxi_state, Ref, TaxiId, Status, Loc} ->
            collect_states(Ref, N - 1, [{TaxiId, Status, Loc} | Acc])
    after ?QUERY_TIMEOUT ->
        Acc   % some taxis did not answer in time; proceed with the rest
    end.

%%%=====================================================================
%%% INTERNAL HELPERS
%%%=====================================================================

%% Squared Euclidean distance: order-equivalent to the Euclidean distance
%% for ranking, and avoids floating point.
sq_dist({X1, Y1}, {X2, Y2}) ->
    DX = X1 - X2, DY = Y1 - Y2,
    DX * DX + DY * DY.

taxi_pid(TaxiId, State) ->
    case lists:keyfind(TaxiId, 1, State#state.taxis) of
        {TaxiId, Pid} -> Pid;
        false -> undefined
    end.

remove_taxi_pid(Pid, State) ->
    State#state{taxis = lists:keydelete(Pid, 2, State#state.taxis)}.

set_trip_state(TripId, NewSt, Active) ->
    case lists:keyfind(TripId, 1, Active) of
        {TripId, TaxiId, TaxiPid, Name, PassPid, Origin, _Old} ->
            lists:keyreplace(TripId, 1, Active,
                {TripId, TaxiId, TaxiPid, Name, PassPid, Origin, NewSt});
        false -> Active
    end.

find_trip_by_name(Name, Active) ->
    case lists:filter(fun(T) -> element(4, T) =:= Name end, Active) of
        [T | _] -> T;
        []      -> false
    end.

remove_trip_by_name(Name, Active) ->
    lists:filter(fun(T) -> element(4, T) =/= Name end, Active).

passenger_status(Name, Active) ->
    case find_trip_by_name(Name, Active) of
        {TripId, TaxiId, _P, Name, _PP, _O, assigned} ->
            io_lib:format("assigned to taxi ~p (trip ~p), waiting for pickup",
                          [TaxiId, TripId]);
        {TripId, TaxiId, _P, Name, _PP, _O, in_service} ->
            io_lib:format("in service with taxi ~p (trip ~p)", [TaxiId, TripId]);
        false ->
            "waiting for assignment"
    end.

%% Synchronous request/reply against the center for the API helpers.
call_center(Msg, Tag) ->
    case resolve(center) of
        undefined ->
            io:format("No dispatch center is open/reachable.~n"),
            {error, no_center};
        Pid ->
            Pid ! Msg,
            receive
                {Tag, Result} -> Result
            after ?CALL_TIMEOUT ->
                {error, timeout}
            end
    end.

%% Resolve a registered name to a Pid: local table first, then cluster-wide
%% global registry, so callers never need to know the PID or node.
resolve(Name) ->
    case whereis(Name) of
        undefined -> global:whereis_name(Name);
        Pid       -> Pid
    end.

%% Uniform message tracing. Sender prints "Sends: ...", receiver "Receives: ...".
log_send(Msg) -> io:format("Sends: ~s~n", [Msg]).
log_recv(Msg) -> io:format("Receives: ~s~n", [Msg]).
