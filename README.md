Mongoz is a replacement for MongoDB sharding server (mongos)
aimed at higher availability in less-than-absolutely-reliable
network environments at expense of somewhat relaxed
consistency guarantees.

Some distinguishing features of mongoz include:

  - **Hard timeouts**: each operation has to finish in predefined time.
    In case of network failures, clients get their errors early
    (i.e. they don't have to wait for i/o timeout from operating system)
    and thus are much less likely to face a DoS.

  - **Early request retransmission**: if there's more than one replica
    capable of handling a request, mongoz can be configured to retransmit
    a request to another replica *before* a timeout occurs and return whichever
    reply comes first. That way, even a network failure in the middle
    of a performing request does not neccessarily lead to any errors
    reported to clients.

  - **Config caching**: mongoz does not query its config servers
    upon a specific event (like incoming connection or authentication attempt).
    Instead, it keeps its local copy of the whole cluster config and
    synchronizes it periodically with config servers. As a result,
    unavailable config servers (even all of them) still
    don't render the whole cluster unusable.

Mongoz strictly obeys MongoDB wire protocol and thus can be used
as a drop-in replacement for mongos. Moreover, it can freely coexist
with mongos within a same MongoDB cluster if neccessary.

More detailed description of mongoz features can be found in its man page.
