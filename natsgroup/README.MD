# An example NATS group membership API

This example NATS API to track group membership.  Due to the nature of heartbeats, it is not scalable to thousands of nodes without a large timeout, but should work with smaller sets of data.

## TODO

 [ ] Scalability:  Allow strong vs weak (don't need to maintain a membership list) membership.
 [ ] Example usage, Walkthrough code, more comments
 [ ] README.MD