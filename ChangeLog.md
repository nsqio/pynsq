# Changelog

## 0.4.3-alpha 

 * #13 - improve backoff handling

## 0.4.2 - 2013-05-09

 * #25 - add Writer
 * #24 - add is_starved to Reader

## 0.4.1 - 2013-04-22

 * #23 - fix Message export, cleanup IDENTIFY response logging

## 0.4.0 - 2013-04-19

 * #22 - feature negotiation (supported by nsqd v0.2.20+)
        wait 2x heartbeat interval before closing conns
        more logging improvements
 * #21 - configurable heartbeat interval (supported by nsqd v0.2.19+)
 * #17 - add task to all connection related logging; ensure max_in_flight is never < # tasks
 * #16 - always set initial RDY count to 1
 * #15 - run travis tests on more tornado versions
 * #14 - automatically reconnect to nsqd when not using lookupd
 * #12 - cleanup/remove deprecated async=True
 * #9 - redistribute ready state when max_in_flight < num_conns

## 0.3.4 - 2013-02-20
 
 * #10 - fix parameter type for REQ
 * #8 - add tests
 * #6 - fix TOUCH; deprecate `async=True` in favor of always using message instance methods

## 0.3.2 - 2013-01-31

 * #4 - TOUCH support; new message response API (deprecates `finisher`)

## 0.3.1 - 2013-01-07

 * #5 - add/use IDENTIFY
 * #1 - check topic/channel length for validity

## 0.3.0 - 2012-10-31

 * Initial Release
