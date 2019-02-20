 Extensions for Redisson Java library
=====

[Redisson](https://github.com/redisson/redisson) is an amazing client library for Redis DB implementing a lot of interesting feature on top of standard Redis API.
However, some Redis APIs were not implemented (ex. [XINFO](https://redis.io/commands/xinfo)), some Redis APIs were implemeted as-is and they require to improve (ex. [XADD](https://redis.io/commands/xadd) command accepts only one message per call and this approach makes batch ingestion into stream relatively slow).
  
This small library tries to implement some missing functions.

### Release notes
##### v 0.0.2
- Added few validations and unit tests for Batch `XADD` command.
- Implemented `XINFO GROUPS` command.
- Implemented `getLastDeliveredId` helper method for Redis Streams.

##### v 0.0.1
- Implemented batch `XADD` command.
