%%%=====================================================================
%%% TC2037 - Implementation of Computational Methods
%%% Activity 6.2 - Distributed Programming in Erlang
%%% Distributed Taxi Rental System for Travel to an Airport
%%%
%%% MODULE: traveler
%%%
%%% TEAM MEMBERS (fill in before submitting):
%%%   Author: [Full Name - A01234567]
%%%   Author: [Full Name - A01234567]
%%%   Author: [Full Name - A01234567]
%%%
%%% PURPOSE
%%%   A traveler is a customer who requests a taxi to the airport. Each
%%%   request spawns a dedicated "passenger" process that represents the
%%%   traveler for the whole life of the request: it sends the request to
%%%   the dispatch center, waits for the assignment (or unavailability) and
%%%   stays alive until the trip is completed or cancelled - at which point
%%%   the process "completes" (terminates), exactly as the spec requires.
%%%
%%% PROCESS MODEL
%%%   * request_taxi/2 spawns ONE passenger process, registered locally and
%%%     cluster-wide (global:register_name/2) under the traveler's unique
%%%     atom name. Global registration RETURNS `no` if the name is already
%%%     taken anywhere in the cluster, which is how "different names must be
%%%     specified for the traveler to be accepted" is enforced.
%%%   * The passenger process LINKS to the center, so if the center
%%%     terminates the passenger is torn down with it (no orphans).
%%%   * request_taxi/2 returns immediately; the passenger process prints the
%%%     assignment / completion events asynchronously on the traveler's node.
%%%
%%% COMMUNICATION PROTOCOL (traced with Sends:/Receives:)
%%%   passenger -> center : {request_taxi, Name, Origin, self()}
%%%   shell     -> center : {cancel, Name, self()}
%%%   center    -> passenger : {taxi_assigned, TaxiId, TripId} | {no_taxi}
%%%                            {rejected, duplicate} | {cancelled}
%%%                            {trip_completed, TripId}
%%%=====================================================================
-module(traveler).

%% ---- Public API ----
-export([request_taxi/2, cancel_taxi/1]).

%% ---- Spawned entry point ----
-export([init_passenger/4]).

-define(CALL_TIMEOUT, 10000).

%%%=====================================================================
%%% PUBLIC API
%%%=====================================================================

%% Request a taxi. Traveler is a unique atom name; Origin is the pickup
%% location {X,Y}. Spawns the passenger process and returns once it is up.
request_taxi(Traveler, Origin) ->
    case resolve(Traveler) of
        Pid when is_pid(Pid) ->
            io:format("Traveler ~p already has an active request.~n", [Traveler]),
            {error, name_in_use};
        undefined ->
            case resolve(center) of
                undefined ->
                    io:format("No dispatch center reachable; cannot request a "
                              "taxi.~n"),
                    {error, no_center};
                Center ->
                    Parent = self(),
                    Pid = spawn(?MODULE, init_passenger,
                                [Traveler, Origin, Center, Parent]),
                    receive
                        {passenger_ready, Pid} ->
                            {ok, Pid};
                        {passenger_error, Reason} ->
                            io:format("Traveler ~p not accepted: ~p~n",
                                      [Traveler, Reason]),
                            {error, Reason}
                    after ?CALL_TIMEOUT ->
                        {error, timeout}
                    end
            end
    end.

%% Cancel a traveler's request (only succeeds if the service has not started).
cancel_taxi(Traveler) ->
    case resolve(center) of
        undefined ->
            io:format("No dispatch center reachable.~n"),
            {error, no_center};
        Center ->
            log_send(io_lib:format("cancel taxi request for ~p", [Traveler])),
            Center ! {cancel, Traveler, self()},
            receive
                {cancel_result, ok} ->
                    io:format("Request for ~p cancelled.~n", [Traveler]),
                    ok;
                {cancel_result, {error, Reason}} ->
                    io:format("Cannot cancel request for ~p: ~p~n",
                              [Traveler, Reason]),
                    {error, Reason}
            after ?CALL_TIMEOUT ->
                {error, timeout}
            end
    end.

%%%=====================================================================
%%% PASSENGER PROCESS
%%%=====================================================================

%% Runs inside the freshly spawned passenger process.
init_passenger(Name, Origin, Center, Parent) ->
    case global:register_name(Name, self()) of
        yes ->
            catch register(Name, self()),
            link(Center),    % center death -> this passenger is torn down
            Parent ! {passenger_ready, self()},
            log_send(io_lib:format("~p requests taxi from ~p", [Name, Origin])),
            Center ! {request_taxi, Name, Origin, self()},
            wait_assignment(Name);
        no ->
            Parent ! {passenger_error, name_taken}
    end.

%% Phase 1: wait for the center's answer to the request.
wait_assignment(Name) ->
    receive
        {taxi_assigned, TaxiId, TripId} ->
            log_recv(io_lib:format("taxi ~p assigned (trip ~p)", [TaxiId, TripId])),
            io:format(">> Traveler ~p: taxi ~p is on the way (trip ~p).~n",
                      [Name, TaxiId, TripId]),
            wait_completion(Name, TripId);
        {no_taxi} ->
            log_recv("no taxi available"),
            io:format(">> Traveler ~p: no taxi available right now.~n", [Name]);
        {rejected, duplicate} ->
            log_recv("request rejected (duplicate)"),
            io:format(">> Traveler ~p: rejected, name already in use.~n", [Name]);
        {cancelled} ->
            log_recv("request cancelled"),
            io:format(">> Traveler ~p: request cancelled.~n", [Name])
    end.
    %% Returning ends the process -> name auto-unregisters (local + global).

%% Phase 2: assigned; wait until the trip completes or is cancelled.
wait_completion(Name, TripId) ->
    receive
        {trip_completed, TripId} ->
            log_recv(io_lib:format("trip ~p completed", [TripId])),
            io:format(">> Traveler ~p: arrived at the airport. Trip ~p "
                      "completed. Thank you!~n", [Name, TripId]);
        {cancelled} ->
            log_recv("trip cancelled"),
            io:format(">> Traveler ~p: trip ~p cancelled.~n", [Name, TripId])
    end.

%%%=====================================================================
%%% INTERNAL HELPERS
%%%=====================================================================

%% Resolve a registered name to a Pid: local table first, then global.
resolve(Name) ->
    case whereis(Name) of
        undefined -> global:whereis_name(Name);
        Pid       -> Pid
    end.

log_send(Msg) -> io:format("Sends: ~s~n", [Msg]).
log_recv(Msg) -> io:format("Receives: ~s~n", [Msg]).
