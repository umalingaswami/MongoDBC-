---
title: Managing majority write concern waits and processing queued operations
---
This document explains the flow of managing majority write concern waits and processing queued operation times. The flow waits for the earliest queued operation time to be confirmed with majority write concern, processes the operations, and updates the last waited operation time. It runs continuously until shutdown to ensure data durability and consistency.

```mermaid
flowchart TD
  node1["Managing Majority Write Concern Waits and Processing Queued Operations
Check for queued operation times
(Managing Majority Write Concern Waits and Processing Queued Operations)"]:::HeadingStyle
  node2["Managing Majority Write Concern Waits and Processing Queued Operations
Wait asynchronously for new operation time notification if none
(Managing Majority Write Concern Waits and Processing Queued Operations)"]:::HeadingStyle
  node3["Managing Majority Write Concern Waits and Processing Queued Operations
Wait for majority write concern on earliest operation time
(Managing Majority Write Concern Waits and Processing Queued Operations)"]:::HeadingStyle
  node4["Managing Majority Write Concern Waits and Processing Queued Operations
Did wait succeed?
(Managing Majority Write Concern Waits and Processing Queued Operations)"]:::HeadingStyle
  node5["Managing Majority Write Concern Waits and Processing Queued Operations
Update last waited operation time
(Managing Majority Write Concern Waits and Processing Queued Operations)"]:::HeadingStyle
  node6["Managing Majority Write Concern Waits and Processing Queued Operations
Process queued operation times equal to earliest
(Managing Majority Write Concern Waits and Processing Queued Operations)"]:::HeadingStyle

  node1 -->|"No"| node2
  node1 -->|"Yes"| node3
  node3 --> node4
  node4 -->|"Yes"| node5
  node4 -->|"No, but error is earlier op time available"| node6
  node5 --> node1
  node6 --> node1
  click node1 goToHeading "Managing Majority Write Concern Waits and Processing Queued Operations"
  click node2 goToHeading "Managing Majority Write Concern Waits and Processing Queued Operations"
  click node3 goToHeading "Managing Majority Write Concern Waits and Processing Queued Operations"
  click node4 goToHeading "Managing Majority Write Concern Waits and Processing Queued Operations"
  click node5 goToHeading "Managing Majority Write Concern Waits and Processing Queued Operations"
  click node6 goToHeading "Managing Majority Write Concern Waits and Processing Queued Operations"
classDef HeadingStyle fill:#777777,stroke:#333,stroke-width:2px;
```

# Managing Majority Write Concern Waits and Processing Queued Operations

```mermaid
%%{init: {"flowchart": {"defaultRenderer": "elk"}} }%%
flowchart TD
    node1{"Are there queued operation times?"}
    click node1 openCode "src/mongo/db/repl/wait_for_majority_service.cpp:189:191"
    node1 -->|"No"| node2["Wait asynchronously for new operation time notification"]
    click node2 openCode "src/mongo/db/repl/wait_for_majority_service.cpp:190:191"
    node1 -->|"Yes"| node3["Wait for majority write concern on earliest operation time"]
    click node3 openCode "src/mongo/db/repl/wait_for_majority_service.cpp:201:203"
    node3 --> node4{"Did wait succeed?"}
    click node4 openCode "src/mongo/db/repl/wait_for_majority_service.cpp:206:210"
    node4 -->|"Yes"| node5["Update last waited operation time"]
    click node5 openCode "src/mongo/db/repl/wait_for_majority_service.cpp:207:208"
    node4 -->|"No, but error is WaitForMajorityServiceEarlierOpTimeAvailable"| node6["Process queued operation times equal to earliest"]
    click node6 openCode "src/mongo/db/repl/wait_for_majority_service.cpp:211:223"
    node4 -->|"No, other error"| node10["Handle error or retry"]
    click node10 openCode "src/mongo/db/repl/wait_for_majority_service.cpp:204:204"

    subgraph loop1["For each queued operation time equal to the earliest"]
        node6 --> node7{"Has operation time been processed?"}
        click node7 openCode "src/mongo/db/repl/wait_for_majority_service.cpp:217:221"
        node7 -->|"No"| node8["Mark operation as processed and remove from queue"]
        click node8 openCode "src/mongo/db/repl/wait_for_majority_service.cpp:218:219"
        node7 -->|"Yes"| node9["Skip to next operation time"]
        node8 --> node6
        node9 --> node6
    end
classDef HeadingStyle fill:#777777,stroke:#333,stroke-width:2px;

%% Swimm:
%% %%{init: {"flowchart": {"defaultRenderer": "elk"}} }%%
%% flowchart TD
%%     node1{"Are there queued operation times?"}
%%     click node1 openCode "<SwmPath>[src/…/repl/wait_for_majority_service.cpp](src/mongo/db/repl/wait_for_majority_service.cpp)</SwmPath>:189:191"
%%     node1 -->|"No"| node2["Wait asynchronously for new operation time notification"]
%%     click node2 openCode "<SwmPath>[src/…/repl/wait_for_majority_service.cpp](src/mongo/db/repl/wait_for_majority_service.cpp)</SwmPath>:190:191"
%%     node1 -->|"Yes"| node3["Wait for majority write concern on earliest operation time"]
%%     click node3 openCode "<SwmPath>[src/…/repl/wait_for_majority_service.cpp](src/mongo/db/repl/wait_for_majority_service.cpp)</SwmPath>:201:203"
%%     node3 --> node4{"Did wait succeed?"}
%%     click node4 openCode "<SwmPath>[src/…/repl/wait_for_majority_service.cpp](src/mongo/db/repl/wait_for_majority_service.cpp)</SwmPath>:206:210"
%%     node4 -->|"Yes"| node5["Update last waited operation time"]
%%     click node5 openCode "<SwmPath>[src/…/repl/wait_for_majority_service.cpp](src/mongo/db/repl/wait_for_majority_service.cpp)</SwmPath>:207:208"
%%     node4 -->|"No, but error is <SwmToken path="src/mongo/db/repl/wait_for_majority_service.cpp" pos="210:10:10" line-data="               if (status != ErrorCodes::WaitForMajorityServiceEarlierOpTimeAvailable) {">`WaitForMajorityServiceEarlierOpTimeAvailable`</SwmToken>"| node6["Process queued operation times equal to earliest"]
%%     click node6 openCode "<SwmPath>[src/…/repl/wait_for_majority_service.cpp](src/mongo/db/repl/wait_for_majority_service.cpp)</SwmPath>:211:223"
%%     node4 -->|"No, other error"| node10["Handle error or retry"]
%%     click node10 openCode "<SwmPath>[src/…/repl/wait_for_majority_service.cpp](src/mongo/db/repl/wait_for_majority_service.cpp)</SwmPath>:204:204"
%% 
%%     subgraph loop1["For each queued operation time equal to the earliest"]
%%         node6 --> node7{"Has operation time been processed?"}
%%         click node7 openCode "<SwmPath>[src/…/repl/wait_for_majority_service.cpp](src/mongo/db/repl/wait_for_majority_service.cpp)</SwmPath>:217:221"
%%         node7 -->|"No"| node8["Mark operation as processed and remove from queue"]
%%         click node8 openCode "<SwmPath>[src/…/repl/wait_for_majority_service.cpp](src/mongo/db/repl/wait_for_majority_service.cpp)</SwmPath>:218:219"
%%         node7 -->|"Yes"| node9["Skip to next operation time"]
%%         node8 --> node6
%%         node9 --> node6
%%     end
%% classDef HeadingStyle fill:#777777,stroke:#333,stroke-width:2px;
```

This section manages the waiting for majority write concern on queued operation times and processes those operations once the write concern is satisfied or an error occurs.

| Category        | Rule Name                           | Description                                                                                                                                                    |
| --------------- | ----------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Data validation | Idempotent processing of operations | Each queued operation time equal to the earliest is checked to ensure it has not been processed before marking it as processed and removing it from the queue. |
| Business logic  | Wait for earliest operation time    | When there are queued operation times, the system waits for the majority write concern on the earliest operation time before processing.                       |
| Business logic  | Update last waited operation time   | If the wait for majority write concern succeeds, the system updates the last waited operation time to the current earliest operation time.                     |
| Business logic  | Continuous processing loop          | The processing loop runs continuously until the system is shut down, ensuring all queued operations are eventually handled.                                    |

<SwmSnippet path="/src/mongo/db/repl/wait_for_majority_service.cpp" line="185">

---

We start the flow by binding a client from <SwmToken path="src/mongo/db/repl/wait_for_majority_service.cpp" pos="187:7:7" line-data="               auto clientGuard = _waitForMajorityClient-&gt;bind();">`_waitForMajorityClient`</SwmToken> to create an operation context for waiting on write concern asynchronously. The function checks if there are any queued operation times; if none, it waits on a condition variable. If there are queued times, it picks the lowest one, unlocks the mutex to wait for the majority write concern on that operation time, then locks the mutex again to process all requests with that operation time. It marks requests as processed, sets their results, and removes them from the queue. This loop runs continuously until shutdown.

```c++
SemiFuture<void> WaitForMajorityService::_periodicallyWaitForMajority() {
    return AsyncTry([this] {
               auto clientGuard = _waitForMajorityClient->bind();
               stdx::unique_lock<Latch> lk(_mutex);
               if (_queuedOpTimes.empty()) {
                   return _hasNewOpTimeCV.onNotify();
               }
               auto opCtx = clientGuard->makeOperationContext();

               // This needs to be a copy since we unlock the lock before waiting for write concern
               // and the iterator could be invalidated.
               auto lowestOpTime = _queuedOpTimes.begin()->first;

               lk.unlock();

               WriteConcernResult ignoreResult;
               auto status = waitForWriteConcern(
                   opCtx.get(), lowestOpTime, kMajorityWriteConcern, &ignoreResult);

               lk.lock();

               if (status.isOK()) {
                   _lastOpTimeWaited = lowestOpTime;
               }

               if (status != ErrorCodes::WaitForMajorityServiceEarlierOpTimeAvailable) {
                   auto [lowestOpTimeIter, firstElemWithHigherOpTimeIter] =
                       _queuedOpTimes.equal_range(lowestOpTime);

                   for (auto requestIt = lowestOpTimeIter;
                        requestIt != firstElemWithHigherOpTimeIter;
                        /*Increment in loop*/) {
                       if (!requestIt->second->hasBeenProcessed.swap(true)) {
                           requestIt->second->result.setFrom(status);
                           requestIt = _queuedOpTimes.erase(requestIt);
                       } else {
                           ++requestIt;
                       }
                   }
```

---

</SwmSnippet>

&nbsp;

*This is an auto-generated document by Swimm 🌊 and has not yet been verified by a human*

<SwmMeta version="3.0.0" repo-id="Z2l0aHViJTNBJTNBTW9uZ29EQkMtJTNBJTNBdW1hbGluZ2Fzd2FtaQ==" repo-name="MongoDBC-"><sup>Powered by [Swimm](https://app.swimm.io/)</sup></SwmMeta>
