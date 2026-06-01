%%% TC2037 - Implementation of Computational Methods
%%% Activity 6.2 - Distributed Programming in Erlang
%%% Distributed Taxi Rental System for Travel to an Airport
%%%
%%% MODULE: center  (Taxi Dispatch Center)
%%%
%%% TEAM MEMBERS (fill in before submitting):
%%%   Author: Luis Alvaro Rosales Salazar - A01255674
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
%%%
%%% =====================================================================
%%% HOW TO RUN AND TEST  (deployment + full command sequence per node)
%%% =====================================================================
%%% The system is distributed: the center, the taxis and the travelers can
%%% each run on their OWN Erlang node. Every inter-process message prints a
%%% "Sends: ..." line on the SENDER's node and a "Receives: ..." line on the
%%% RECEIVER's node, so the message trace is naturally split across terminals.
%%%
%%% 0) COMPILE (in the project directory, on every machine that runs a node):
%%%      erlc center.erl taxi.erl traveler.erl
%%%    -> produces center.beam, taxi.beam, traveler.beam.
%%%
%%% 1) START ONE NODE PER TERMINAL, ALL WITH THE SAME COOKIE:
%%%      Terminal 1 (center): erl -sname center -setcookie taxi
%%%      Terminal 2 (taxis) : erl -sname taxis  -setcookie taxi
%%%      Terminal 3 (riders): erl -sname riders -setcookie taxi
%%%    On separate machines use -name center@FQDN etc. (same scheme on all
%%%    nodes), keep epmd (TCP 4369) reachable, and the same cookie everywhere.
%%%    Each shell prompt shows its node name, e.g. (center@host)1>; use that
%%%    host wherever <host> appears below.
%%%
%%% 2) OPEN THE CENTER (on the center node). It registers locally as 'center':
%%%      (center@host)1> center:open_center({0,0}).   %% airport at {0,0}
%%%
%%% 3) FETCH THE CENTER PID ON THE OTHER NODES and bind it to C; pass C to
%%%    every traveler/center call (taxi commands address taxis by their id):
%%%      (taxis@host)1>  C = center:find('center@<host>').
%%%      (riders@host)1> C = center:find('center@<host>').
%%%    SINGLE NODE? Do it all in one shell: bind C = center:open_center({0,0}).
%%%    and skip center:find -- the same C is used everywhere below.
%%%
%%% 4) FULL COMMAND SEQUENCE  (the (node) prefix says where to type each one;
%%%    TripId is the center's counter shown in the offer line -- use it when
%%%    calling accept_trip/reject_trip):
%%%
%%%    -- REGISTER TAXIS (each call spawns one taxi process) --
%%%    (taxis)  taxi:register_taxi(t1, {1,1}, C).  %% t1 active, available at {1,1}
%%%    (taxis)  taxi:register_taxi(t2, {9,9}, C).  %% t2 active, available at {9,9}
%%%    (taxis)  taxi:register_taxi(t1, {1,1}, C).  %% duplicate id -> {error, already_registered}
%%%    (taxis)  taxi:consult_taxi(t2).             %% show t2 status + location
%%%    (taxis)  taxi:current_location(t2, {8,8}).  %% move t2 to {8,8}
%%%    (center) center:taxi_list(C).               %% both taxis listed as available
%%%
%%%    -- HAPPY PATH: request -> assign closest -> run -> complete (trip 1) --
%%%    (riders) traveler:request_taxi(jose, {1,2}, C). %% closest taxi (t1) is offered trip 1
%%%    (taxis)  taxi:accept_trip(t1, 1).               %% t1 accepts -> occupied
%%%    (center) center:travelers_list(C).              %% jose "assigned to taxi t1"
%%%    (taxis)  taxi:service_started(t1).              %% t1 picks up at {1,2} -> in_service
%%%    (taxis)  taxi:service_completed(t1).            %% drop at airport -> t1 available at {0,0}
%%%    (center) center:completed_trips(C).             %% trip 1 in history, counter = 1
%%%
%%%    -- REJECT -> NEXT-CLOSEST FALLBACK, then DUPLICATE NAME (trip 2) --
%%%    (riders) traveler:request_taxi(maria, {10,10}, C). %% closest (t2) offered trip 2
%%%    (taxis)  taxi:reject_trip(t2, 2).                  %% t2 rejects -> center offers next taxi
%%%    (taxis)  taxi:accept_trip(t1, 2).                  %% t1 takes trip 2 (occupied)
%%%    (riders) traveler:request_taxi(maria, {0,0}, C).   %% maria still active -> {rejected, duplicate}
%%%
%%%    -- CANCEL BEFORE SERVICE STARTS: allowed (trip 3) --
%%%    (riders) traveler:request_taxi(ana, {2,2}, C). %% only t2 available -> offered trip 3
%%%    (taxis)  taxi:accept_trip(t2, 3).              %% assigned, not started
%%%    (riders) traveler:cancel_taxi(ana, C).         %% ok -> t2 freed (trip 3 cancelled)
%%%
%%%    -- CANCEL AFTER SERVICE STARTS: refused; REMOVE OCCUPIED: refused (trip 4) --
%%%    (riders) traveler:request_taxi(edu, {2,2}, C). %% t2 offered trip 4
%%%    (taxis)  taxi:accept_trip(t2, 4).
%%%    (taxis)  taxi:service_started(t2).             %% in_service
%%%    (riders) traveler:cancel_taxi(edu, C).         %% {error, service_already_started}
%%%    (taxis)  taxi:remove_taxi(t2).                 %% {error, occupied}
%%%
%%%    -- FINISH the open trips so the taxis become available again --
%%%    (taxis)  taxi:service_started(t1).             %% maria's trip 2 starts
%%%    (taxis)  taxi:service_completed(t1).           %% t1 available
%%%    (taxis)  taxi:service_completed(t2).           %% edu's trip 4 done -> t2 available
%%%    (center) center:completed_trips(C).            %% trips 1,2,4 completed (3 cancelled), counter = 4
%%%
%%%    -- REMOVE AVAILABLE: succeeds; then SHUTDOWN --
%%%    (taxis)  taxi:remove_taxi(t1).   %% available -> taxi process ends, center drops its entry
%%%    (center) center:close_center(C). %% center stops; every taxi/passenger monitoring it stops too
%%% =====================================================================
-module(center).

%% Public API (run in the CALLER's process).
-export([open_center/1, find/1, close_center/1,
         taxi_list/1, travelers_list/1, completed_trips/1]).

%% Spawned entry point.
-export([init/1]).

%% Timeouts (ms): 10000 = wait for a center reply; 60000 = wait for a taxi's
%% accept/reject; 2000 = wait for the taxis' status replies.

%% Center state is a plain hardcoded 6-element list (no record). The fixed
%% positions are, in order:
%%   element 1 = airport      {X,Y}
%%   element 2 = taxis        [{TaxiId, Pid}]   (ONLY id+pid, per the spec)
%%   element 3 = passengers   [{Name, PassPid, Origin}]
%%   element 4 = active       [{TripId,TaxiId,TaxiPid,Name,PassPid,Origin,assigned|in_service}]
%%   element 5 = completed    [{TripId,TaxiId,Name,Origin}] newest first
%%   element 6 = counter      ++ on each accepted assignment
%% Reads use lists:nth(N, State); updates rebuild the whole list so the
%% comment on each line shows exactly which element is being kept or changed.

%% Create the dispatch center at the airport {X,Y}. Returns the center PID.
open_center(Airport) ->
    case whereis(center) of
        undefined ->
            Pid = spawn(center, init, [Airport]),
            register(center, Pid),
            io:format("Taxi Dispatch Center open at airport ~p on node ~p~n",
                      [Airport, node()]),
            io:format("Center PID = ~p~n", [Pid]),
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
    after 10000 -> {error, timeout}
    end.

%% List active taxis with all relevant info (queries each taxi live).
taxi_list(CenterPid) ->
    CenterPid ! {taxi_list, self()},
    receive
        {taxi_list_result, States} ->
            io:format("~nActive taxis (~p):~n", [length(States)]),
            lists:foreach(
              fun({TaxiId, Status, Loc}) ->
                  io:format("  Taxi ~p | status: ~p | location: ~p~n",
                            [TaxiId, Status, Loc])
              end, States),
            ok
    after 10000 -> {error, timeout}
    end.

%% List the current passengers.
travelers_list(CenterPid) ->
    CenterPid ! {travelers_list, self()},
    receive
        {travelers_list_result, Passengers, Active} ->
            io:format("~nCurrent passengers (~p):~n", [length(Passengers)]),
            lists:foreach(
              fun({Name, _Pid, Origin}) ->
                  io:format("  Passenger ~p | origin: ~p | ~s~n",
                            [Name, Origin, passenger_status(Name, Active)])
              end, Passengers),
            ok
    after 10000 -> {error, timeout}
    end.

%% Show the history of completed trips and the trip counter.
completed_trips(CenterPid) ->
    CenterPid ! {completed_trips, self()},
    receive
        {completed_trips_result, Counter, Completed} ->
            io:format("~nCompleted trips (assignments: ~p, completed: ~p):~n",
                      [Counter, length(Completed)]),
            lists:foreach(
              fun({TripId, TaxiId, Name, Origin}) ->
                  io:format("  Trip ~p | taxi: ~p | passenger: ~p | origin: ~p~n",
                            [TripId, TaxiId, Name, Origin])
              end, lists:reverse(Completed)),
            ok
    after 10000 -> {error, timeout}
    end.

%%% PROCESS LOOP

init(Airport) ->
    %% Build the initial center state as a plain 6-element list:
    %%   [airport, taxis, passengers, active, completed, counter]
    loop([Airport, [], [], [], [], 0]).

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
            %% rebuild the state, only changing element 4 = active
            loop([lists:nth(1, State),   % element 1 = airport
                  lists:nth(2, State),   % element 2 = taxis
                  lists:nth(3, State),   % element 3 = passengers
                  set_trip_state(TripId, in_service, lists:nth(4, State)), % element 4 = active (updated)
                  lists:nth(5, State),   % element 5 = completed
                  lists:nth(6, State)]); % element 6 = counter

        {service_completed, TaxiId, TripId} ->
            log_recv(io_lib:format("service completed by taxi ~p (trip ~p)",
                                   [TaxiId, TripId])),
            loop(handle_completed(TaxiId, TripId, State));

        {'DOWN', _Ref, process, Pid, Reason} ->
            loop(handle_down(Pid, Reason, State));

        {taxi_list, From} ->
            From ! {taxi_list_result,
                    query_taxis(lists:nth(2, State))},  % element 2 = taxis
            loop(State);

        {travelers_list, From} ->
            From ! {travelers_list_result,
                    lists:nth(3, State),    % element 3 = passengers
                    lists:nth(4, State)},   % element 4 = active
            loop(State);

        {completed_trips, From} ->
            From ! {completed_trips_result,
                    lists:nth(6, State),    % element 6 = counter
                    lists:nth(5, State)},   % element 5 = completed
            loop(State);

        {close, From} ->
            log_recv("close_center request"),
            io:format("Closing center: ~p taxi(s) will stop via their "
                      "monitors~n", [length(lists:nth(2, State))]), % element 2 = taxis
            From ! {closed, self()};
            %% returning here stops the loop -> center process ends ->
            %% taxis/passengers monitoring it receive 'DOWN' and stop.

        Other ->
            io:format("Center: ignoring unexpected message ~p~n", [Other]),
            loop(State)
    end.

%%% REQUEST HANDLING / ASSIGNMENT

handle_request(Name, Origin, PassPid, State) ->
    case lists:keymember(Name, 1, lists:nth(3, State)) of  % element 3 = passengers
        true ->
            log_send(io_lib:format("reject ~p (duplicate passenger)", [Name])),
            PassPid ! {rejected, duplicate},
            State;
        false ->
            %% rebuild the state, only changing element 3 = passengers
            S1 = [lists:nth(1, State),   % element 1 = airport
                  lists:nth(2, State),   % element 2 = taxis
                  [{Name, PassPid, Origin} | lists:nth(3, State)], % element 3 = passengers (updated)
                  lists:nth(4, State),   % element 4 = active
                  lists:nth(5, State),   % element 5 = completed
                  lists:nth(6, State)],  % element 6 = counter
            dispatch(Name, Origin, PassPid, S1)
    end.

%% Query taxis, keep the available ones sorted by distance, offer in order.
dispatch(Name, Origin, PassPid, State) ->
    Ranked = lists:sort([ {sq_dist(Loc, Origin), TaxiId}
                          || {TaxiId, available, Loc}
                                 <- query_taxis(lists:nth(2, State)) ]), % element 2 = taxis
    Candidates = [ {TaxiId, taxi_pid(TaxiId, State)} || {_D, TaxiId} <- Ranked ],
    TripId = lists:nth(6, State) + 1,   % element 6 = counter
    offer_loop(Candidates, TripId, Name, Origin, PassPid, State).

offer_loop([], _TripId, Name, _Origin, PassPid, State) ->
    log_send(io_lib:format("no taxi available for ~p", [Name])),
    PassPid ! {no_taxi},
    %% rebuild the state, only changing element 3 = passengers
    [lists:nth(1, State),   % element 1 = airport
     lists:nth(2, State),   % element 2 = taxis
     lists:keydelete(Name, 1, lists:nth(3, State)), % element 3 = passengers (updated)
     lists:nth(4, State),   % element 4 = active
     lists:nth(5, State),   % element 5 = completed
     lists:nth(6, State)];  % element 6 = counter
offer_loop([{TaxiId, TaxiPid} | Rest], TripId, Name, Origin, PassPid, State) ->
    log_send(io_lib:format("trip offer ~p to taxi ~p (origin ~p)",
                           [TripId, TaxiId, Origin])),
    TaxiPid ! {offer, TripId, Name, Origin},
    %% TaxiId and TaxiPid are already bound from THIS offer, so the patterns
    %% below only match a response carrying that same TaxiId (and PID) -- the
    %% center never mistakes which taxi is replying to the request.
    receive
        {trip_response, accept, TripId, TaxiId, TaxiPid} ->
            log_recv(io_lib:format("taxi ~p ACCEPTED trip ~p", [TaxiId, TripId])),
            log_send(io_lib:format("taxi ~p assigned to ~p (trip ~p)",
                                   [TaxiId, Name, TripId])),
            PassPid ! {taxi_assigned, TaxiId, TripId},
            Trip = {TripId, TaxiId, TaxiPid, Name, PassPid, Origin, assigned},
            %% rebuild the state, changing element 4 = active and element 6 = counter
            [lists:nth(1, State),         % element 1 = airport
             lists:nth(2, State),         % element 2 = taxis
             lists:nth(3, State),         % element 3 = passengers
             [Trip | lists:nth(4, State)],% element 4 = active (updated)
             lists:nth(5, State),         % element 5 = completed
             TripId];                     % element 6 = counter (updated)
        {trip_response, reject, TripId, TaxiId, TaxiPid} ->
            log_recv(io_lib:format("taxi ~p REJECTED trip ~p - trying next",
                                   [TaxiId, TripId])),
            offer_loop(Rest, TripId, Name, Origin, PassPid, State);
        {'DOWN', _Ref, process, TaxiPid, _Reason} ->
            log_recv(io_lib:format("taxi ~p died during offer - trying next",
                                   [TaxiId])),
            offer_loop(Rest, TripId, Name, Origin, PassPid,
                       remove_taxi_pid(TaxiPid, State))
    after 60000 ->
        log_recv(io_lib:format("taxi ~p did not answer - trying next", [TaxiId])),
        offer_loop(Rest, TripId, Name, Origin, PassPid, State)
    end.

%%% OTHER HANDLERS

handle_register(TaxiId, TaxiPid, State) ->
    case lists:keymember(TaxiId, 1, lists:nth(2, State)) of  % element 2 = taxis
        true ->
            TaxiPid ! {register_error, already_registered},
            State;
        false ->
            erlang:monitor(process, TaxiPid),    % taxi dies -> we get 'DOWN'
            log_send(io_lib:format("airport ~p to taxi ~p",
                                   [lists:nth(1, State), TaxiId])), % element 1 = airport
            TaxiPid ! {registered, lists:nth(1, State)},  % element 1 = airport
            %% rebuild the state, only changing element 2 = taxis
            [lists:nth(1, State),   % element 1 = airport
             [{TaxiId, TaxiPid} | lists:nth(2, State)], % element 2 = taxis (updated)
             lists:nth(3, State),   % element 3 = passengers
             lists:nth(4, State),   % element 4 = active
             lists:nth(5, State),   % element 5 = completed
             lists:nth(6, State)]   % element 6 = counter
    end.

handle_completed(TaxiId, TripId, State) ->
    case lists:keyfind(TripId, 1, lists:nth(4, State)) of  % element 4 = active
        {TripId, TaxiId, _TaxiPid, Name, PassPid, Origin, _St} ->
            log_send(io_lib:format("trip ~p completed -> passenger ~p",
                                   [TripId, Name])),
            PassPid ! {trip_completed, TripId},
            %% rebuild the state, changing elements 3, 4 and 5
            [lists:nth(1, State),   % element 1 = airport
             lists:nth(2, State),   % element 2 = taxis
             lists:keydelete(Name, 1, lists:nth(3, State)),   % element 3 = passengers (updated)
             lists:keydelete(TripId, 1, lists:nth(4, State)), % element 4 = active (updated)
             [{TripId, TaxiId, Name, Origin} | lists:nth(5, State)], % element 5 = completed (updated)
             lists:nth(6, State)];  % element 6 = counter
        _ ->
            State
    end.

handle_cancel(Name, From, State) ->
    case find_trip_by_name(Name, lists:nth(4, State)) of  % element 4 = active
        {TripId, _TaxiId, TaxiPid, Name, PassPid, _Origin, assigned} ->
            log_send(io_lib:format("cancel trip ~p (free taxi, notify passenger)",
                                   [TripId])),
            TaxiPid ! {trip_cancelled, TripId},
            PassPid ! {cancelled},
            From ! {cancel_result, ok},
            %% rebuild the state, changing element 3 = passengers and element 4 = active
            [lists:nth(1, State),   % element 1 = airport
             lists:nth(2, State),   % element 2 = taxis
             lists:keydelete(Name, 1, lists:nth(3, State)),   % element 3 = passengers (updated)
             lists:keydelete(TripId, 1, lists:nth(4, State)), % element 4 = active (updated)
             lists:nth(5, State),   % element 5 = completed
             lists:nth(6, State)];  % element 6 = counter
        {_TripId, _TaxiId, _TaxiPid, Name, _PassPid, _Origin, in_service} ->
            From ! {cancel_result, {error, service_already_started}},
            State;
        false ->
            case lists:keyfind(Name, 1, lists:nth(3, State)) of  % element 3 = passengers
                {Name, PassPid, _O} ->
                    PassPid ! {cancelled},
                    From ! {cancel_result, ok},
                    %% rebuild the state, only changing element 3 = passengers
                    [lists:nth(1, State),   % element 1 = airport
                     lists:nth(2, State),   % element 2 = taxis
                     lists:keydelete(Name, 1, lists:nth(3, State)), % element 3 = passengers (updated)
                     lists:nth(4, State),   % element 4 = active
                     lists:nth(5, State),   % element 5 = completed
                     lists:nth(6, State)];  % element 6 = counter
                false ->
                    From ! {cancel_result, {error, not_found}},
                    State
            end
    end.

%% A monitored taxi process went down: drop only its entry; center survives.
handle_down(Pid, Reason, State) ->
    case lists:keyfind(Pid, 2, lists:nth(2, State)) of  % element 2 = taxis
        {TaxiId, Pid} ->
            log_recv(io_lib:format("taxi ~p process down (~p) - removing entry",
                                   [TaxiId, Reason])),
            %% rebuild the state, only changing element 2 = taxis
            [lists:nth(1, State),   % element 1 = airport
             lists:keydelete(Pid, 2, lists:nth(2, State)), % element 2 = taxis (updated)
             lists:nth(3, State),   % element 3 = passengers
             lists:nth(4, State),   % element 4 = active
             lists:nth(5, State),   % element 5 = completed
             lists:nth(6, State)];  % element 6 = counter
        false ->
            State
    end.

%%% TAXI STATUS QUERY  (center keeps only id+pid, so it asks the taxis)

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
    after 2000 ->
        Acc
    end.

%%% HELPERS

%% Squared distance: same ordering as Euclidean, no floating point needed.
sq_dist({X1, Y1}, {X2, Y2}) -> (X1 - X2) * (X1 - X2) + (Y1 - Y2) * (Y1 - Y2).

taxi_pid(TaxiId, State) ->
    {TaxiId, Pid} = lists:keyfind(TaxiId, 1, lists:nth(2, State)), % element 2 = taxis
    Pid.

remove_taxi_pid(Pid, State) ->
    %% rebuild the state, only changing element 2 = taxis
    [lists:nth(1, State),   % element 1 = airport
     lists:keydelete(Pid, 2, lists:nth(2, State)), % element 2 = taxis (updated)
     lists:nth(3, State),   % element 3 = passengers
     lists:nth(4, State),   % element 4 = active
     lists:nth(5, State),   % element 5 = completed
     lists:nth(6, State)].  % element 6 = counter

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
