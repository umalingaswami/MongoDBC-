---
title: NetworkInterfaceTL in Task Executor
---
# Overview of <SwmToken path="src/mongo/executor/network_interface_tl.cpp" pos="458:2:2" line-data="Status NetworkInterfaceTL::startCommand(const TaskExecutor::CallbackHandle&amp; cbHandle,">`NetworkInterfaceTL`</SwmToken>

<SwmToken path="src/mongo/executor/network_interface_tl.cpp" pos="458:2:2" line-data="Status NetworkInterfaceTL::startCommand(const TaskExecutor::CallbackHandle&amp; cbHandle,">`NetworkInterfaceTL`</SwmToken> is a concrete implementation of the <SwmToken path="src/mongo/executor/network_interface_tl.cpp" pos="103:2:2" line-data="                                                    &quot;NetworkInterface shutdown in progress&quot;};">`NetworkInterface`</SwmToken> designed to manage network operations within the Task Executor component. It orchestrates the entire lifecycle of network commands, including their initiation, cancellation, and scheduling, while also managing alarms and timers that coordinate timed network activities.

# Connection Management and Reactor Thread

To optimize network communication, <SwmToken path="src/mongo/executor/network_interface_tl.cpp" pos="458:2:2" line-data="Status NetworkInterfaceTL::startCommand(const TaskExecutor::CallbackHandle&amp; cbHandle,">`NetworkInterfaceTL`</SwmToken> maintains a connection pool that allows efficient reuse of network connections. It supports SSL configurations to ensure secure data transmission. A dedicated reactor thread runs asynchronously to process network events, enabling non-blocking operations that improve overall responsiveness and throughput.

# Command and Alarm Lifecycle Management

<SwmToken path="src/mongo/executor/network_interface_tl.cpp" pos="458:2:2" line-data="Status NetworkInterfaceTL::startCommand(const TaskExecutor::CallbackHandle&amp; cbHandle,">`NetworkInterfaceTL`</SwmToken> tracks all in-progress commands and alarms to guarantee proper cancellation and resource cleanup, especially during shutdown sequences. It employs internal data structures such as <SwmToken path="src/mongo/executor/network_interface_tl.cpp" pos="493:12:12" line-data="    auto [cmdState, future] = CommandState::make(this, request, cbHandle);">`CommandState`</SwmToken> and <SwmToken path="src/mongo/executor/network_interface_tl.cpp" pos="333:5:5" line-data="AsyncDBClient* NetworkInterfaceTL::RequestState::getClient(const ConnectionHandle&amp; conn) noexcept {">`RequestState`</SwmToken> to maintain detailed state information and manage the lifecycle of individual network requests effectively.

# Diagnostic and Statistical Capabilities

The interface provides comprehensive diagnostic and statistical data about network activity, including connection statistics and counters. These metrics are valuable for monitoring network performance and troubleshooting issues within the Task Executor's network operations.

<SwmSnippet path="/src/mongo/executor/network_interface_tl.cpp" line="458">

---

An illustrative example is the <SwmToken path="src/mongo/executor/network_interface_tl.cpp" pos="458:4:4" line-data="Status NetworkInterfaceTL::startCommand(const TaskExecutor::CallbackHandle&amp; cbHandle,">`startCommand`</SwmToken> method, which initiates a network command by creating a <SwmToken path="src/mongo/executor/network_interface_tl.cpp" pos="493:12:12" line-data="    auto [cmdState, future] = CommandState::make(this, request, cbHandle);">`CommandState`</SwmToken> object responsible for managing the request's lifecycle. This method sends the request asynchronously through the connection pool and reactor thread, returning a status that indicates whether the command was successfully started or if it encountered an error. This example demonstrates how <SwmToken path="src/mongo/executor/network_interface_tl.cpp" pos="458:2:2" line-data="Status NetworkInterfaceTL::startCommand(const TaskExecutor::CallbackHandle&amp; cbHandle,">`NetworkInterfaceTL`</SwmToken> integrates command execution with its internal networking mechanisms to provide efficient and reliable network communication.

```c++
Status NetworkInterfaceTL::startCommand(const TaskExecutor::CallbackHandle& cbHandle,
                                        RemoteCommandRequestOnAny& request,
                                        RemoteCommandCompletionFn&& onFinish,
                                        const BatonHandle& baton) try {
    if (inShutdown()) {
        return kNetworkInterfaceShutdownInProgress;
    }

    LOGV2_DEBUG(
        22596, kDiagnosticLogLevel, "startCommand", "request"_attr = redact(request.toString()));

    if (_metadataHook) {
        BSONObjBuilder newMetadata(std::move(request.metadata));

        auto status = _metadataHook->writeRequestMetadata(request.opCtx, &newMetadata);
        if (!status.isOK()) {
            return status;
        }

        request.metadata = newMetadata.obj();
    }

    bool targetHostsInAlphabeticalOrder =
        MONGO_unlikely(networkInterfaceSendRequestsToTargetHostsInAlphabeticalOrder.shouldFail(
            [request](const BSONObj&) { return request.hedgeOptions != boost::none; }));

    if (targetHostsInAlphabeticalOrder) {
        // Sort the target hosts by host names.
        std::sort(request.target.begin(),
                  request.target.end(),
                  [](const HostAndPort& target1, const HostAndPort& target2) {
                      return target1.toString() < target2.toString();
                  });
    }

    auto [cmdState, future] = CommandState::make(this, request, cbHandle);
    if (cmdState->requestOnAny.timeout != cmdState->requestOnAny.kNoTimeout) {
        cmdState->deadline = cmdState->stopwatch.start() + cmdState->requestOnAny.timeout;
    }
    cmdState->baton = baton;

    if (_svcCtx && cmdState->requestOnAny.hedgeOptions) {
        auto hm = HedgingMetrics::get(_svcCtx);
        invariant(hm);
        hm->incrementNumTotalOperations();
    }

    // When our command finishes, run onFinish out of line.
    std::move(future)
        // Run the callback on the baton if it exists and is not shut down, and run on the reactor
        // otherwise.
        .thenRunOn(makeGuaranteedExecutor(baton, _reactor))
        .getAsync([cmdState = cmdState,
                   onFinish = std::move(onFinish)](StatusWith<RemoteCommandOnAnyResponse> swr) {
            invariant(swr.isOK());
            auto rs = std::move(swr.getValue());
            // The TransportLayer has, for historical reasons returned
            // SocketException for network errors, but sharding assumes
            // HostUnreachable on network errors.
            if (rs.status == ErrorCodes::SocketException) {
                rs.status = Status(ErrorCodes::HostUnreachable, rs.status.reason());
            }

            LOGV2_DEBUG(22597,
                        2,
                        "Request finished with response",
                        "requestId"_attr = cmdState->requestOnAny.id,
                        "isOK"_attr = rs.isOK(),
                        "response"_attr =
                            redact(rs.isOK() ? rs.data.toString() : rs.status.toString()));
            onFinish(std::move(rs));
        });

    if (MONGO_unlikely(networkInterfaceDiscardCommandsBeforeAcquireConn.shouldFail())) {
        LOGV2(22598, "Discarding command due to failpoint before acquireConn");
        return Status::OK();
    }

    // Attempt to get a connection to every target host
    for (size_t idx = 0; idx < request.target.size(); ++idx) {
        auto connFuture = _pool->get(request.target[idx], request.sslMode, request.timeout);

        // If connection future is ready or requests should be sent in order, send the request
        // immediately.
        if (connFuture.isReady() || targetHostsInAlphabeticalOrder) {
            cmdState->requestManager->trySend(std::move(connFuture).getNoThrow(), idx);
            continue;
        }

        // Otherwise, schedule the request.
        std::move(connFuture).thenRunOn(_reactor).getAsync([cmdState = cmdState, idx](auto swConn) {
            cmdState->requestManager->trySend(std::move(swConn), idx);
        });
    }

    return Status::OK();
} catch (const DBException& ex) {
    return ex.toStatus();
}
```

---

</SwmSnippet>

&nbsp;

*This is an auto-generated document by Swimm 🌊 and has not yet been verified by a human*

<SwmMeta version="3.0.0" repo-id="Z2l0aHViJTNBJTNBTW9uZ29EQkMtJTNBJTNBdW1hbGluZ2Fzd2FtaQ==" repo-name="MongoDBC-"><sup>Powered by [Swimm](https://app.swimm.io/)</sup></SwmMeta>
