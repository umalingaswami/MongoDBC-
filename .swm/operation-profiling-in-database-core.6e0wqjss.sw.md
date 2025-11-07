---
title: Operation Profiling in Database Core
---
# Overview of Operation Profiling

Operation profiling in the database core is a mechanism designed to monitor and record detailed information about each database operation's execution. This process captures essential performance metrics such as execution time, the number of keys and documents examined, and occurrences of write conflicts. These metrics are vital for diagnosing performance issues and optimizing database behavior.

# Role of the CurOp Class

At the heart of operation profiling is the CurOp class, which manages the lifecycle of every database operation. CurOp tracks key details including the operation's start and end times, the namespace it operates on, and its progress status. It supports nested operations by maintaining a stack of active CurOp instances per client, ensuring that sub-operations are accurately profiled within the context of their parent operations.

# Metrics Collection and Aggregation

During the execution of an operation, CurOp gathers various metrics such as keys examined, documents examined, and write conflicts. These metrics are aggregated within the OpDebug::AdditiveMetrics structure, which consolidates performance data to provide a comprehensive view of the operation's resource usage and behavior.

# Integration with OperationContext

The profiling system is tightly integrated with the OperationContext, linking profiling data directly to the current operation being executed. This association ensures that profiling information is accurately attributed and can be retrieved or analyzed in relation to specific operations.

# Profiling Lifecycle Methods

CurOp offers methods to manage the profiling lifecycle, including starting and stopping timing, setting operation-specific details, and finalizing profiling data. Upon completion of an operation, CurOp logs the collected profiling information, which can then be serialized into BSON format for reporting and further analysis.

# Selective Profiling with ProfileFilter

To optimize performance and focus on relevant operations, the ProfileFilter interface allows selective profiling. It defines criteria based on the operation's context and debug information to determine which operations should be included in profiling. This selective approach helps reduce overhead by excluding less significant operations from profiling.

# Serialization and Reporting

Profiling data collected by CurOp is serialized into BSON objects, facilitating detailed inspection and analysis of operation performance. This serialized data supports various reporting tools and diagnostic processes, enabling developers and administrators to understand and improve database operation efficiency.

# Example Implementation in Source Code

The source file <SwmPath>[src/…/db/curop.cpp](src/mongo/db/curop.cpp)</SwmPath> contains the implementation of the CurOp class and its profiling lifecycle. It initializes profiling data when an operation begins, updates metrics throughout execution, and completes profiling by logging the aggregated data. The OpDebug class within this file aggregates key metrics such as keys examined and write conflicts, providing a detailed performance overview for each operation.

&nbsp;

*This is an auto-generated document by Swimm 🌊 and has not yet been verified by a human*

<SwmMeta version="3.0.0" repo-id="Z2l0aHViJTNBJTNBTW9uZ29EQkMtJTNBJTNBdW1hbGluZ2Fzd2FtaQ==" repo-name="MongoDBC-"><sup>Powered by [Swimm](https://app.swimm.io/)</sup></SwmMeta>
