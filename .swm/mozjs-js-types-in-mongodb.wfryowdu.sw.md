---
title: Mozjs JS Types in MongoDB
---
# Overview of Mozjs JS Types

Mozjs JS Types are C++ template-based wrappers designed to expose native C++ classes and functions to the JavaScript environment embedded within MongoDB. They provide a bridge that allows JavaScript code to interact seamlessly with MongoDB's underlying C++ implementation.

# Purpose and Functionality

These wrappers manage the entire lifecycle of the exposed types, including construction, property access, method invocation, and garbage collection tracing. By doing so, they ensure that JavaScript code can safely and efficiently use native C++ functionality without dealing with the complexities of language interoperability.

# Wrapping Mechanism

The wrapping mechanism leverages advanced C++ template programming and macros to abstract the complexity of binding C++ code to JavaScript. This includes converting exceptions thrown in C++ into JavaScript exceptions, preventing exception leakage across language boundaries and maintaining robust error handling.

# JavaScript Object Behavior Support

JS Types implement standard JavaScript object behaviors such as adding, deleting, and enumerating properties, invoking methods, constructing new instances, and tracing for garbage collection. These behaviors are implemented as template methods specialized for each wrapped C++ type, ensuring consistent and predictable interaction patterns.

# Installation and Exposure

Wrapped types can be installed either into the JavaScript global scope or privately within specific contexts. This controlled exposure is achieved through methods that attach functions and properties to the JavaScript global object or to particular prototypes, allowing fine-grained control over the availability of native functionality.

# Example Usage in the Codebase

In the MongoDB source, the `WrapType` template class located in <SwmPath>[src/…/mozjs/wraptype.h](src/mongo/scripting/mozjs/wraptype.h)</SwmPath> provides key methods such as `install()`, `newObject()`, and `newInstance()`. These methods facilitate creating and exposing wrapped C++ types to JavaScript. Additionally, macros like `MONGO_ATTACH_JS_FUNCTION` are used to attach C++ functions as callable JavaScript functions, simplifying the binding process.

&nbsp;

*This is an auto-generated document by Swimm 🌊 and has not yet been verified by a human*

<SwmMeta version="3.0.0" repo-id="Z2l0aHViJTNBJTNBTW9uZ29EQkMtJTNBJTNBdW1hbGluZ2Fzd2FtaQ==" repo-name="MongoDBC-"><sup>Powered by [Swimm](https://app.swimm.io/)</sup></SwmMeta>
