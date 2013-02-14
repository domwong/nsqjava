# nsqjava

A [Netty](https://github.com/netty/netty) based implementation for interacting with [NSQ](https://github.com/bitly/nsq)

### Getting started

Pull down the repo and build locally. This project is Maven-ified so a simple 'mvn compile package install' should suffice.

There is an example publisher and subscriber in the nsqjava.examples package

### Missing features

  - Interaction with the NSQ lookup daemon for service discovery.
  - Any sort of automatic resilience for publishing. You currently need to manually set up multiple connections and publish to them. Ideally you'll want to have a wrapper object which handles publishing to N queues.

### Disclaimer

This is still a very rough cut so disclaimers apply; if this borks your system it's not my fault (blah, blah, etc...). 
