# Distributed Taxi Rental System (Erlang)

**TC2037 – Implementation of Computational Methods**
**Activity 6.2 – Distributed Programming in Erlang**

> Team members (fill in before submitting):
> - `[Full Name – A01234567]`
> - `[Full Name – A01234567]`
> - `[Full Name – A01234567]`

A distributed taxi‑dispatch system for travel to an airport. Travelers request a
taxi from a central dispatcher, which assigns the **closest available** taxi by
Euclidean distance; taxis accept/reject offers and run the transport service; the
center keeps the registry of taxis, the current passengers, the trips in progress
and the history of completed trips. Every inter‑process message is traced with
`Sends:` / `Receives:` lines on the node where each process runs.

---

## 1. Modules

| Module | Role | Process model |
|--------|------|---------------|
| `center.erl`   | Taxi Dispatch Center | one process, registered as `center` (local **and** `global`). Traps exits and links every taxi. |
| `taxi.erl`     | Taxis | one process per taxi, registered under its `TaxiId` (local + `global`). Does **not** trap exits. |
| `traveler.erl` | Travelers | one *passenger* process per request, registered under the traveler name (local + `global`), linked to the center. |

### Public interface

**`center`**
- `center:open_center(AirportLocation)` – create the dispatcher (e.g. `{0,0}`).
- `center:close_center()` – terminate the dispatcher (all taxis die with it).
- `center:taxi_list()` – list active taxis with live status + location.
- `center:travelers_list()` – list current passengers.
- `center:completed_trips()` – history of completed trips + trip counter.

**`taxi`**
- `taxi:register_taxi(TaxiId, InitialLocation)` – register/activate a taxi.
- `taxi:current_location(TaxiId, Location)` – update a taxi's location.
- `taxi:accept_trip(TripId)` / `taxi:reject_trip(TripId)` – answer a trip offer.
- `taxi:service_started(TaxiId)` – notify start of service (taxi occupied at origin).
- `taxi:service_completed(TaxiId)` – notify end of service (taxi available at airport).
- `taxi:remove_taxi(TaxiId)` – remove a taxi (only when **available**).
- `taxi:consult_taxi(TaxiId)` – show a taxi's status and location.

**`traveler`**
- `traveler:request_taxi(Traveler, Origin)` – request a taxi (`Traveler` is a unique
  atom, `Origin` is `{X,Y}`).
- `traveler:cancel_taxi(Traveler)` – cancel a request **before** its service starts.

---

## 2. Why the API has no node argument

The center, taxis and travelers register their names **cluster‑wide** with
`global:register_name/2`. Any process resolves a name with:

```erlang
resolve(Name) ->
    case whereis(Name) of          % fast local table first
        undefined -> global:whereis_name(Name);  % then the cluster‑wide registry
        Pid       -> Pid
    end.
```

so callers never need the PID **or** the node of the center/taxis. Replies travel to
the `self()` PID carried in each message, which is location‑transparent across nodes.
`global:register_name/2` also returns `no` when a name is already taken, which is how
**unique traveler names** and **unique TaxiIds** are enforced.

---

## 3. Compile (on every node's machine)

From the project directory:

```bash
erlc center.erl taxi.erl traveler.erl
```

This produces `center.beam`, `taxi.beam`, `traveler.beam`. Start each Erlang node
from that same directory (or add `-pa <dir>` so the modules are on the code path).

---

## 4. Start the nodes (one terminal each, shared cookie)

Use **the same magic cookie** on every node. This demo uses four nodes; open one
terminal per node:

```bash
# Terminal 1 – the dispatch center
erl -sname center -setcookie taxi_demo

# Terminal 2 – a taxi node
erl -sname taxi1  -setcookie taxi_demo

# Terminal 3 – a second taxi node
erl -sname taxi2  -setcookie taxi_demo

# Terminal 4 – a traveler node
erl -sname trav1  -setcookie taxi_demo
```

> Multiple machines? Replace `-sname X` with `-name x@FQDN` on **every** node (do not
> mix `-sname` and `-name`), make sure `epmd` (TCP 4369) and the distribution port
> range are reachable, and use the same cookie. On a single machine `-sname` is enough.

Each shell prompt shows its node name, e.g. `(center@your-host)1>`. Use **that host
name** wherever `<host>` appears below.

### Connect the cluster (before opening the center)

Erlang connects nodes lazily, and `global` only syncs names across **connected**
nodes. From every non‑center node, ping the center once:

```erlang
%% on taxi1, taxi2 and trav1:
net_adm:ping('center@<host>').    %% expect: pong
nodes().                          %% should list the other connected nodes
```

After the center is open you can confirm the shared registry from any node:

```erlang
global:registered_names().        %% e.g. [center, t1, t2, jose, ...]
```

---

## 5. Full command sequence (which node runs what)

Run the commands in this order. The **node** column says where to type each command;
the **output** column says where the `Sends:` / `Receives:` traces appear.

| # | Node      | Command | Effect (output node) |
|---|-----------|---------|----------------------|
| 1 | `center`  | `center:open_center({0,0}).` | Center opens at the airport `{0,0}`. |
| 2 | `taxi1`   | `taxi:register_taxi(t1, {1,1}).` | Taxi `t1` active. `Sends:` on taxi1, `Receives:` on center. |
| 3 | `taxi2`   | `taxi:register_taxi(t2, {9,9}).` | Taxi `t2` active. |
| 4 | `trav1`   | `traveler:request_taxi(jose, {1,2}).` | `Sends:` on trav1, `Receives:` on center; center offers to the **closest** taxi (`t1`). |
| 5 | `taxi1`   | `taxi:accept_trip(1).` | `t1` accepts trip `1`; center answers `jose` with `{t1, 1}`. |
| 6 | `taxi1`   | `taxi:service_started(t1).` | `t1` occupied at the origin `{1,2}`. |
| 7 | `taxi1`   | `taxi:service_completed(t1).` | `t1` available at the airport `{0,0}`; `jose`'s process completes. |
| 8 | `center`  | `center:taxi_list().` | Lists `t1`, `t2` with live status + location. |
| 9 | `center`  | `center:travelers_list().` | Current passengers (empty now). |
| 10| `center`  | `center:completed_trips().` | Shows trip `1` (`t1`, `jose`, `{1,2}`) and the counter. |
| 11| `taxi1`   | `taxi:consult_taxi(t1).` | Shows `t1` available at `{0,0}`. |

### Show the reject → next‑closest fallback

| # | Node    | Command | Effect |
|---|---------|---------|--------|
| 12| `trav1` | `traveler:request_taxi(maria, {10,10}).` | Center offers to `t1` first (closer to `{10,10}`). |
| 13| `taxi1` | `taxi:reject_trip(2).` | `t1` rejects → center offers trip `2` to `t2`. |
| 14| `taxi2` | `taxi:accept_trip(2).` | `t2` accepts; `maria` assigned to `t2`. |

### Show cancellation (before service starts) and update location

| # | Node    | Command | Effect |
|---|---------|---------|--------|
| 15| `trav1` | `traveler:request_taxi(ana, {2,2}).` | Offered to the only available taxi (`t1`). |
| 16| `taxi1` | `taxi:accept_trip(3).` | `t1` assigned to `ana` (not started). |
| 17| `trav1` | `traveler:cancel_taxi(ana).` | Trip cancelled; `t1` becomes available again. |
| 18| `taxi1` | `taxi:current_location(t1, {4,4}).` | `t1` moves to `{4,4}`. |

### Show removal rules and shutdown

| # | Node    | Command | Effect |
|---|---------|---------|--------|
| 19| `taxi2` | `taxi:remove_taxi(t2).` | **Fails** – `t2` is occupied with `maria`. |
| 20| `taxi1` | `taxi:remove_taxi(t1).` | Succeeds – `t1` is available; center drops its entry. |
| 21| `center`| `center:close_center().` | Center terminates; **every remaining taxi process dies automatically**. |

---

## 6. Expected output (excerpt)

When `jose` requests a taxi (steps 4–5), the trace is **spread across the nodes**:

On **trav1**:
```
Sends: jose requests taxi from {1,2}
>> Traveler jose: taxi t1 is on the way (trip 1).
```

On **center**:
```
Receives: taxi request from jose
Sends: status query to 1 taxi(s)
Sends: trip offer 1 to taxi t1 (origin {1,2})
Receives: taxi t1 ACCEPTED trip 1
Sends: taxi t1 assigned to jose (trip 1)
```

On **taxi1**:
```
Receives: trip offer 1 from passenger jose (origin {1,2})
   -> To respond: taxi:accept_trip(1)  OR  taxi:reject_trip(1)
Sends: accept trip 1
Receives: assignment of trip 1 (origin {1,2}) - now OCCUPIED
```

`center:completed_trips()` prints, for example:
```
--- COMPLETED TRIPS (total assignments: 1, completed: 1) ---
  Trip 1 | taxi: t1 | passenger: jose | origin: {1,2}
-----------------------------------------------
```

> **Why is output split across terminals?** Every `Sends:` line is printed by the
> *sending* process and therefore appears on the node where that process runs; every
> `Receives:` line is printed by the *receiving* process. Because the center, taxis
> and travelers live on different nodes, the message trace is naturally distributed —
> which is exactly the distributed behavior this activity demonstrates. (If you drive
> the system from a single controlling shell via `rpc:call/4`, Erlang funnels the I/O
> back to that shell instead — type the commands in each node's own shell to see the
> per‑node split.)

---

## 7. Message protocol (summary)

```
traveler  -> center : {request_taxi, Name, Origin, PassPid}
                      {cancel, Name, From}
center    -> taxi   : {get_state, Self, Ref}        % status query
                      {offer, TripId, Name, Origin}  % trip offer
                      {assign, TripId, Origin}       % commit (taxi -> occupied)
                      {registered, Self, Airport}    % registration ack
                      {trip_cancelled, TripId}       % free a reserved taxi
taxi      -> center : {register_taxi, TaxiId, Pid, Loc}
                      {taxi_state, Ref, TaxiId, Status, Loc}
                      {trip_response, accept|reject, TripId, Pid}
                      {service_started,   TaxiId, TripId}
                      {service_completed, TaxiId, TripId}
center    -> trav   : {taxi_assigned, TaxiId, TripId} | {no_taxi}
                      {rejected, duplicate} | {cancelled}
                      {trip_completed, TripId}
```

**Assignment** – on a request the center queries every taxi for its live
`{status, location}` (it stores only `{TaxiId, Pid}`), keeps the *available* ones,
sorts them by Euclidean distance to the origin and offers the trip from closest to
farthest. A rejection (or a non‑answer, or the offered taxi dying) makes it try the
next‑closest. The trip counter increments only when an offer is **accepted**.

---

## 8. Lifecycle guarantees

- **The center traps exits and links every taxi.**
  - A taxi process dying → the center receives `{'EXIT', Pid, _}` and only removes
    that taxi's entry; the center keeps running.
  - The center dying with a non‑normal reason → the exit signal propagates over the
    links and kills every (non‑trapping) taxi **automatically**.
- **`close_center()`** kills each taxi explicitly and then exits with reason
  `shutdown`, so a `normal` exit cannot leave orphan taxis. Passenger processes are
  linked to the center, so they are torn down as well.
- Names are removed automatically when their process dies (both the local table and
  `global` clean up on death).

---

## 9. Troubleshooting

| Symptom | Cause / fix |
|---------|-------------|
| `traveler/taxi` prints *"No dispatch center reachable"* | The node is not connected, or the center is not open. Run `net_adm:ping('center@<host>')` and `center:open_center(...)`. |
| `nodes()` is empty on a taxi/traveler node | You forgot to `net_adm:ping` the center, or the cookies differ. Start every node with the same `-setcookie`. |
| `pang` from `net_adm:ping` | Wrong host/short‑name, different cookie, or `epmd` not reachable. Check the prompt's node name and the cookie. |
| Mixed `-sname`/`-name` nodes won't connect | Use the same naming scheme (all short, or all long) on every node. |
| A taxi can't be removed | `remove_taxi/1` only works when the taxi is **available** (not mid‑service). |
