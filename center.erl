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
%%%   The Taxi Dispatch Center is a single process that centralizes the
%%%   information of the system: the active taxis, the current passengers,
%%%   the trips in progress and the history of completed trips. It assigns
%%%   the closest AVAILABLE taxi (Euclidean distance) to each request,
%%%   generates a unique trip id per assignment and keeps a trip counter.
%%%
%%% PROCESS MODEL  (plain, PID-based message passing)
%%%   * open_center/1 spawns ONE process and returns its PID. The PID is how
%%%     travelers and taxis talk to the center: you always send to a PID.
%%%   * The only registered name is `center`, registered LOCALLY on the
%%%     center's own node, so that a shell on another node can fetch the PID
%%%     once with center:find/1 (rpc whereis) and then pass that PID around.
%%%   * Lifecycle uses erlang:monitor (no links, no trap_exit):
%%%       - the center monitors every taxi: a taxi dying delivers
%%%         {'DOWN',_,process,Pid,_} and the center just drops that entry;
%%%       - each taxi (and passenger) monitors the center and stops itself
%%%         when the center goes DOWN, so closing/crashing the center tears
%%%         the whole system down automatically.
%%%   * Per the spec the center stores ONLY {TaxiId, Pid} per taxi; the live
%%%     status/location lives in each taxi process, so the center QUERIES the
%%%     taxis (get_state) when it needs to sort by distance or list them.
%%%=====================================================================
-module(center).

%% Public API (run in the CALLER's process).
-export([open_center/1, find/1, close_center/1,
         taxi_list/1, travelers_list/1, completed_trips/1]).

%% Spawned entry point.
-export([init/1]).

-define(CALL_TIMEOUT, 10000).   % wait for a center reply (ms)
-define(OFFER_TIMEOUT, 60000).  % wait for a taxi's accept/reject (ms)
-define(QUERY_TIMEOUT, 2000).   % wait for the taxis' status replies (ms)

%% Center state. `taxis` holds ONLY {TaxiId, Pid} as required by the spec.
-record(state, {airport,            % {X,Y}
                taxis      = [],     % [{TaxiId, Pid}]
                passengers = [],     % [{Name, PassPid, Origin}]
                active     = [],     % [{TripId,TaxiId,TaxiPid,Name,PassPid,Origin,assigned|in_service}]
                completed  = [],     % [{TripId,TaxiId,Name,Origin}] newest first
                counter    = 0}).    % ++ on each accepted assignment

%%%=====================================================================
%%% PUBLIC API
%%%=====================================================================

%% Create the dispatch center at the airport {X,Y}. Returns the center PID.
open_center(Airport) ->
    case whereis(center) of
        undefined ->
            Pid = spawn(?MODULE, init, [Airport]),
            register(center, Pid),
            io:format("== Taxi Dispatch Center OPEN at airport ~p on node ~p ==~n",
                      [Airport, node()]),
            io:format("   Center PID = ~p  (other nodes: use center:find(~p))~n",
                      [Pid, node()]),
            Pid;
        Pid ->
            io:format("Center already open with PID ~p~n", [Pid]),
            Pid
    end.

%% Bootstrap helper: fetch the center PID from another node's shell.
%%   On a taxi/traveler node:  C = center:find('center@host').
find(Node) ->
    case rpc:call(Node, erlang, whereis, [center]) of
        Pid when is_pid(Pid) -> Pid;
        Other ->
            io:format("No center found on ~p (~p)~n", [Node, Other]),
            undefined
    end.

%% Terminate the dispatch center (taxis/passengers stop via their monitors).
close_center(CenterPid) ->
    CenterPid ! {close, self()},
    receive
        {closed, CenterPid} -> ok
    after ?CALL_TIMEOUT -> {error, timeout}
    end.

%% List active taxis with all relevant info (queries each taxi live).
taxi_list(CenterPid) ->
    CenterPid ! {taxi_list, self()},
    receive
        {taxi_list_result, States} ->
            io:format("~n--- ACTIVE TAXIS (~p) ---~n", [length(States)]),
            lists:foreach(
              fun({TaxiId, Status, Loc}) ->
                  io:format("  Taxi ~p | status: ~p | location: ~p~n",
                            [TaxiId, Status, Loc])
              end, States),
            io:format("-------------------------~n"),
            ok
    after ?CALL_TIMEOUT -> {error, timeout}
    end.

%% List the current passengers.
travelers_list(CenterPid) ->
    CenterPid ! {travelers_list, self()},
    receive
        {travelers_list_result, Passengers, Active} ->
            io:format("~n--- CURRENT PASSENGERS (~p) ---~n", [length(Passengers)]),
            lists:foreach(
              fun({Name, _Pid, Origin}) ->
                  io:format("  Passenger ~p | origin: ~p | ~s~n",
                            [Name, Origin, passenger_status(Name, Active)])
              end, Passengers),
            io:format("-------------------------------~n"),
            ok
    after ?CALL_TIMEOUT -> {error, timeout}
    end.

%% Show the history of completed trips and the trip counter.
completed_trips(CenterPid) ->
    CenterPid ! {completed_trips, self()},
    receive
        {completed_trips_result, Counter, Completed} ->
            io:format("~n--- COMPLETED TRIPS (assignments: ~p, completed: ~p) ---~n",
                      [Counter, length(Completed)]),
            lists:foreach(
              fun({TripId, TaxiId, Name, Origin}) ->
                  io:format("  Trip ~p | taxi: ~p | passenger: ~p | origin: ~p~n",
                            [TripId, TaxiId, Name, Origin])
              end, lists:reverse(Completed)),
            io:format("-----------------------------------------------~n"),
            ok
    after ?CALL_TIMEOUT -> {error, timeout}
    end.

%%%=====================================================================
%%% PROCESS LOOP
%%%=====================================================================

init(Airport) ->
    loop(#state{airport = Airport}).

loop(State) ->
    receive
        {request_taxi, Name, Origin, PassPid} ->
            log_recv(io_lib:format("taxi request from ~p", [Name])),
            loop(handle_request(Name, Origin, PassPid, State));

        {cancel, Name, From} ->
            log_recv(io_lib:format("cancel request from ~p", [Name])),
            loop(handle_cancel(Name, From, State));

        {register_taxi, TaxiId, TaxiPid, Loc} ->
            log_recv(io_lib:format("register taxi ~p at ~p", [TaxiId, Loc])),
            loop(handle_register(TaxiId, TaxiPid, State));

        {service_started, TaxiId, TripId} ->
            log_recv(io_lib:format("service started by taxi ~p (trip ~p)",
                                   [TaxiId, TripId])),
            loop(State#state{active =
                     set_trip_state(TripId, in_service, State#state.active)});

        {service_completed, TaxiId, TripId} ->
            log_recv(io_lib:format("service completed by taxi ~p (trip ~p)",
                                   [TaxiId, TripId])),
            loop(handle_completed(TaxiId, TripId, State));

        {'DOWN', _Ref, process, Pid, Reason} ->
            loop(handle_down(Pid, Reason, State));

        {taxi_list, From} ->
            From ! {taxi_list_result, query_taxis(State#state.taxis)},
            loop(State);

        {travelers_list, From} ->
            From ! {travelers_list_result,
                    State#state.passengers, State#state.active},
            loop(State);

        {completed_trips, From} ->
            From ! {completed_trips_result,
                    State#state.counter, State#state.completed},
            loop(State);

        {close, From} ->
            log_recv("close_center request"),
            io:format("== Closing center: ~p taxi(s) will stop via their "
                      "monitors ==~n", [length(State#state.taxis)]),
            From ! {closed, self()};
            %% returning here stops the loop -> center process ends ->
            %% taxis/passengers monitoring it receive 'DOWN' and stop.

        Other ->
            io:format("Center: ignoring unexpected message ~p~n", [Other]),
            loop(State)
    end.

%%%=====================================================================
%%% REQUEST HANDLING / ASSIGNMENT
%%%=====================================================================

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

%% Query taxis, keep the available ones sorted by distance, offer in order.
dispatch(Name, Origin, PassPid, State) ->
    Ranked = lists:sort([ {sq_dist(Loc, Origin), TaxiId}
                          || {TaxiId, available, Loc}
                                 <- query_taxis(State#state.taxis) ]),
    Candidates = [ {TaxiId, taxi_pid(TaxiId, State)} || {_D, TaxiId} <- Ranked ],
    TripId = State#state.counter + 1,
    offer_loop(Candidates, TripId, Name, Origin, PassPid, State).

offer_loop([], _TripId, Name, _Origin, PassPid, State) ->
    log_send(io_lib:format("no taxi available for ~p", [Name])),
    PassPid ! {no_taxi},
    State#state{passengers = lists:keydelete(Name, 1, State#state.passengers)};
offer_loop([{TaxiId, TaxiPid} | Rest], TripId, Name, Origin, PassPid, State) ->
    log_send(io_lib:format("trip offer ~p to taxi ~p (origin ~p)",
                           [TripId, TaxiId, Origin])),
    TaxiPid ! {offer, TripId, Name, Origin},
    receive
        {trip_response, accept, TripId, TaxiId, TaxiPid} ->
            log_recv(io_lib:format("taxi ~p ACCEPTED trip ~p", [TaxiId, TripId])),
            log_send(io_lib:format("taxi ~p assigned to ~p (trip ~p)",
                                   [TaxiId, Name, TripId])),
            PassPid ! {taxi_assigned, TaxiId, TripId},
            Trip = {TripId, TaxiId, TaxiPid, Name, PassPid, Origin, assigned},
            State#state{counter = TripId, active = [Trip | State#state.active]};
        {trip_response, reject, TripId, TaxiId, TaxiPid} ->
            log_recv(io_lib:format("taxi ~p REJECTED trip ~p - trying next",
                                   [TaxiId, TripId])),
            offer_loop(Rest, TripId, Name, Origin, PassPid, State);
        {'DOWN', _Ref, process, TaxiPid, _Reason} ->
            log_recv(io_lib:format("taxi ~p died during offer - trying next",
                                   [TaxiId])),
            offer_loop(Rest, TripId, Name, Origin, PassPid,
                       remove_taxi_pid(TaxiPid, State))
    after ?OFFER_TIMEOUT ->
        log_recv(io_lib:format("taxi ~p did not answer - trying next", [TaxiId])),
        offer_loop(Rest, TripId, Name, Origin, PassPid, State)
    end.

%%%=====================================================================
%%% OTHER HANDLERS
%%%=====================================================================

handle_register(TaxiId, TaxiPid, State) ->
    case lists:keymember(TaxiId, 1, State#state.taxis) of
        true ->
            TaxiPid ! {register_error, already_registered},
            State;
        false ->
            erlang:monitor(process, TaxiPid),    % taxi dies -> we get 'DOWN'
            log_send(io_lib:format("airport ~p to taxi ~p",
                                   [State#state.airport, TaxiId])),
            TaxiPid ! {registered, State#state.airport},
            State#state{taxis = [{TaxiId, TaxiPid} | State#state.taxis]}
    end.

handle_completed(TaxiId, TripId, State) ->
    case lists:keyfind(TripId, 1, State#state.active) of
        {TripId, TaxiId, _TaxiPid, Name, PassPid, Origin, _St} ->
            log_send(io_lib:format("trip ~p completed -> passenger ~p",
                                   [TripId, Name])),
            PassPid ! {trip_completed, TripId},
            State#state{
              active     = lists:keydelete(TripId, 1, State#state.active),
              passengers = lists:keydelete(Name, 1, State#state.passengers),
              completed  = [{TripId, TaxiId, Name, Origin} | State#state.completed]};
        _ ->
            State
    end.

handle_cancel(Name, From, State) ->
    case find_trip_by_name(Name, State#state.active) of
        {TripId, _TaxiId, TaxiPid, Name, PassPid, _Origin, assigned} ->
            log_send(io_lib:format("cancel trip ~p (free taxi, notify passenger)",
                                   [TripId])),
            TaxiPid ! {trip_cancelled, TripId},
            PassPid ! {cancelled},
            From ! {cancel_result, ok},
            State#state{
              active     = lists:keydelete(TripId, 1, State#state.active),
              passengers = lists:keydelete(Name, 1, State#state.passengers)};
        {_TripId, _TaxiId, _TaxiPid, Name, _PassPid, _Origin, in_service} ->
            From ! {cancel_result, {error, service_already_started}},
            State;
        false ->
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

%% A monitored taxi process went down: drop only its entry; center survives.
handle_down(Pid, Reason, State) ->
    case lists:keyfind(Pid, 2, State#state.taxis) of
        {TaxiId, Pid} ->
            log_recv(io_lib:format("taxi ~p process down (~p) - removing entry",
                                   [TaxiId, Reason])),
            State#state{taxis = lists:keydelete(Pid, 2, State#state.taxis)};
        false ->
            State
    end.

%%%=====================================================================
%%% TAXI STATUS QUERY  (center keeps only id+pid, so it asks the taxis)
%%%=====================================================================

query_taxis(Taxis) ->
    Ref = make_ref(),
    lists:foreach(fun({_Id, Pid}) -> Pid ! {get_state, self(), Ref} end, Taxis),
    log_send(io_lib:format("status query to ~p taxi(s)", [length(Taxis)])),
    collect_states(Ref, length(Taxis), []).

collect_states(_Ref, 0, Acc) -> Acc;
collect_states(Ref, N, Acc) ->
    receive
        {taxi_state, Ref, TaxiId, Status, Loc} ->
            collect_states(Ref, N - 1, [{TaxiId, Status, Loc} | Acc])
    after ?QUERY_TIMEOUT ->
        Acc
    end.

%%%=====================================================================
%%% HELPERS
%%%=====================================================================

%% Squared distance: same ordering as Euclidean, no floating point needed.
sq_dist({X1, Y1}, {X2, Y2}) -> (X1 - X2) * (X1 - X2) + (Y1 - Y2) * (Y1 - Y2).

taxi_pid(TaxiId, State) ->
    {TaxiId, Pid} = lists:keyfind(TaxiId, 1, State#state.taxis),
    Pid.

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

log_send(Msg) -> io:format("Sends: ~s~n", [Msg]).
log_recv(Msg) -> io:format("Receives: ~s~n", [Msg]).
