These KSQL are to demonstrate
1. how to do windowed aggregatio in KSQL
2. how KSQL has a bug where when input records might not trigger a window close where it should have
   1. ticket: https://support.confluent.io/hc/en-us/requests/161091?page=1
   2. Steps to produce
      1. create ksql_source stream
      2. create ksql_output table
      3. start monitoring source and output
      4. insert two records in one go (by copy-paste two INSERT into KSQL cli and run them together), where the two timestamp fields are 12 minutes apart
      5. observed: under platform 7.4, no window close, but if the two INSERTs are run one after another, the window is closed and result emitted.
