# MSK.BEAT BackEnd v.1.0.0 <J.A.R.V.I.S.>

Dependencies:

* Databases:

    * redis
    * MySQLdb
    * PySQLPool


* Networking

    * requests
    * requests\[security\] (pyOpenSSL, ndg-httpsclient, pyasn1)
    * TwitterAPI

* UI

    * npyscreen

* Math and DataScience

    * sklearn
    * networkx
    * numpy


Files:
1. collector.py - threading thread. TBD - overwrite. For now consists of three network connectors (Instagram, Twitter, VKontakte) and console UI.
2. detector.py - data processor, event detector + emulator (need to be exported to a separate file - TBD).
3. detector_legacy.py - ancient project for event detector, currently abandoned, but some code bieces could be useful.
