%%% TC2037 - Implementation of Computational Methods
%%% Activity 6.2 - Distributed Programming in Erlang
%%% Distributed Taxi Rental System for Travel to an Airport
%%%
%%% MODULE: traveler
%%%
%%% TEAM MEMBERS (fill in before submitting):
%%%   Author: Luis Alvaro Rosales Salazar - A01255674
%%%   Author: [Full Name - A01234567]
%%%   Author: [Full Name - A01234567]
%%%
%%% PURPOSE
%%%   A traveler requests a taxi to the airport. Each request spawns a
%%%   dedicated "passenger" process that sends the request to the center,
%%%   waits for the assignment (or unavailability) and stays alive until the
%%%   trip is completed or cancelled - then the process ends.
%%%
%%% PROCESS MODEL  (plain, PID-based)
%%%   * request_taxi/3 spawns the passenger process and RETURNS its PID.
%%%     CenterPid comes from open_center/1 or center:find/1.
%%%   * The passenger monitors the center; if the center goes DOWN it stops.
%%%   * No name is registered: the unique traveler name is just data, and the
%%%     center enforces uniqueness against its own passenger list.
%%%
%%% HOW TO RUN AND TEST: see the "HOW TO RUN AND TEST" comment block at the
%%% top of center.erl for the per-node deployment steps and the full command
%%% sequence that exercises every interface function of all three modules.
-module(traveler).

-export([request_taxi/3, cancel_taxi/2]).
-export([init_passenger/4]).

%%% PUBLIC API

%% Request a taxi. Traveler is a unique atom name, Origin is {X,Y}.
%% Returns the passenger process PID.
request_taxi(Traveler, Origin, CenterPid) ->
    Parent = self(),
    Pid = spawn(traveler, init_passenger, [Traveler, Origin, CenterPid, Parent]),
    receive
        {passenger_ready, Pid} -> Pid
    after 10000 -> {error, timeout}
    end.

%% Cancel a traveler's request (only works before the service starts).
cancel_taxi(Traveler, CenterPid) ->
    log_send(io_lib:format("cancel taxi request for ~p", [Traveler])),
    CenterPid ! {cancel, Traveler, self()},
    receive
        {cancel_result, ok} ->
            io:format("Request for ~p cancelled.~n", [Traveler]), ok;
        {cancel_result, {error, Reason}} ->
            io:format("Cannot cancel ~p: ~p~n", [Traveler, Reason]),
            {error, Reason}
    after 10000 -> {error, timeout}
    end.

%%% PASSENGER PROCESS

init_passenger(Name, Origin, CenterPid, Parent) ->
    erlang:monitor(process, CenterPid),    % center dies -> passenger stops
    Parent ! {passenger_ready, self()},
    log_send(io_lib:format("~p requests taxi from ~p", [Name, Origin])),
    CenterPid ! {request_taxi, Name, Origin, self()},
    wait_assignment(Name, CenterPid).

%% Phase 1: wait for the center's answer.
wait_assignment(Name, CenterPid) ->
    receive
        {taxi_assigned, TaxiId, TripId} ->
            log_recv(io_lib:format("taxi ~p assigned (trip ~p)", [TaxiId, TripId])),
            io:format("Traveler ~p: taxi ~p is on the way (trip ~p).~n",
                      [Name, TaxiId, TripId]),
            wait_completion(Name, TripId, CenterPid);
        {no_taxi} ->
            log_recv("no taxi available"),
            io:format("Traveler ~p: no taxi available right now.~n", [Name]);
        {rejected, duplicate} ->
            log_recv("rejected (duplicate name)"),
            io:format("Traveler ~p: rejected, name already in use.~n", [Name]);
        {cancelled} ->
            log_recv("request cancelled"),
            io:format("Traveler ~p: request cancelled.~n", [Name]);
        {'DOWN', _Ref, process, CenterPid, _Reason} ->
            io:format("Traveler ~p: center is down.~n", [Name])
    end.

%% Phase 2: assigned; wait for completion or cancellation.
wait_completion(Name, TripId, CenterPid) ->
    receive
        {trip_completed, TripId} ->
            log_recv(io_lib:format("trip ~p completed", [TripId])),
            io:format("Traveler ~p: arrived at the airport. Trip ~p done. "
                      "Thank you!~n", [Name, TripId]);
        {cancelled} ->
            log_recv("trip cancelled"),
            io:format("Traveler ~p: trip ~p cancelled.~n", [Name, TripId]);
        {'DOWN', _Ref, process, CenterPid, _Reason} ->
            io:format("Traveler ~p: center is down.~n", [Name])
    end.

log_send(Msg) -> io:format("Sends: ~s~n", [Msg]).
log_recv(Msg) -> io:format("Receives: ~s~n", [Msg]).
