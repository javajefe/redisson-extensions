 Extensions for Redisson Java library
=====

[Redisson](https://github.com/redisson/redisson) is an amazing client library for Redis DB implementing a lot of interesting feature on top of standard Redis API.
However, some Redis APIs were not implemented (ex. [XINFO](https://redis.io/commands/xinfo)), some Redis APIs were implemeted as-is and they require to improve (ex. [XADD](https://redis.io/commands/xadd) command accepts only one message per call and this approach makes batch ingestion into stream relatively slow).
  
This small library tries to implement some missing functions. The approach is very simple: every command is implemented as Lua script.

### Release notes
##### v 0.0.3
- Batch `XADD` now returns list of `StreamMessageId`-s it has inserted into the stream
- Changed Maven `groupId`

##### v 0.0.2
- Added few validations and unit tests for Batch `XADD` command.
- Implemented `XINFO GROUPS` command.
- Implemented `getStreamLastDeliveredId` helper method for Redis Streams. Returns `last-delivered-id` from `XINFO GROUPS` command result.
- Implemented `getStreamTailSize` helper method for Redis streams. Returns number of messages in the stream were not delivered to the reading group yet. 

##### v 0.0.1
- Implemented batch `XADD` command.

### TODO
- Extend input parameters validation.
- API refactoring.