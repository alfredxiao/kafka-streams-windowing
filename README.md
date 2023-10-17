1. does EACH input record cause two deserialization and one serialization
   Yes! can be seen by counting serde
2. does caching, non exactly-once processing help with deser/ser?
   No. exactly-once creates transaction, commit frequently, but still same with deser/ser.
3. does EACH input record cause a write to aggregated store?
   Yes, firstly a deser from store to get the List, then a ser to bytes to write to the store as modified List
4. does memory based store help?
   No, it does not reduce deser/ser computation, although it helps with IO bit
5. how can stats we tell from profiler?
   Yes, can be seen from profiler like async-profiler, VisualVM, where deser/ser uses lots of CPU
   Can also be seen by a comparison between two run settings:
   - Run streaming app with high input volume, e.g. 9000 within same 5 min window
   - Modify the app to only retain 10 records within the List while still allow volume of same amount
   - Result: retaining only 10 records in the List uses a lot less CPU
6. use a faster serde? https://github.com/linkedin/avro-util
   Not yet tested how much can be improved

