# getstats-shipper-to-sqlite
Consumes HTTP POSTs with WebRTC *.getStats() reports and saves to SQLite for real-time reporting or later system-wide WebRTC performance evaluation


This is one-half of a two-part system for capturing *.getStats() reports 
to monitor or understand performance of small or large-scale WebRTC systems.



From example, to show frame decode failures over time, you might do this
```bash
sqlite3 getstats.db 
```

```
select
    pcid,
    json_extract(json,"$.timestamp"),
    json_extract(json,"$.framesReceived") - json_extract(json,"$.framesDecoded")
from getstats 
where json_extract(json,"$.framesReceived") != ""
;
```





