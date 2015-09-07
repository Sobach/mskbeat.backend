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
1. backend.py - main backend file. For now consists of three network connectors (Instagram, Twitter, VKontakte) and console UI.
2. event_detector.py - developing and testing module for online messages processing and event extraction.