# PULSE.MSK.RU BackEnd [Oxenaforda]

## About

PULSE.MSK.RU is a draft of new media: users construct agenda and content through their pesonal actions. 
Social media spreading and geolocation functions allow to get real-time city-buzz. 
Using filtering, clustering, and some ML technics, it's possible to recognize events at the time of occurrence.
Event content is extracted from disparate messages and media objects. Produced news are being classified.

## FAQ [draft]

### Why Oxenaforda?

\- I decided to mark major versions with ancient cities names. Oxenaforda stands for Oxford. And Oxford was selected because of this paper: [Editorial Algorithms: Using Social Media to Discover and Report Local News](http://www.aaai.org/ocs/index.php/ICWSM/ICWSM15/paper/view/10593)

## Change Log

### [Unreleased]

#### Added:

- settings_template.py with comments;

#### Changed:

- Event class: new properties (created, updated, start, end); new methods (is_successor, merge, backup, add_slice).

### [v1.0.0 Oxenaforda] - 2015-12-25

#### Added:

- CollectorEmulator class: emulator for real-time messages Collector for online clustering testing. Using fast_forward ratio it's possible to adjust the speed.

#### Changed:

- EventDetector class: grid approach for outliers detection deprecated. Instead kNN distance is used (via bunch of KDTrees: so-called KDForest (-_-) ).

#### Fixed:

- Entropy calculations in Event class: groupby from itertools require sorting.

#### Removed:

- All grid-related calcualtions and functions.

## Third-party dependencies:

- DATABASES: redis, PySQLPool, MySQLdb;

- CALCULUS: numpy, networkx, sklearn, scipy;

- NLTK: pymorphy2, nltk.

## Files:

- collector.py: contains classes for online data parsing in Instagram, Twitter, and VKontakte. Also has console UI. TBD: remove console UI; rewrite output queues (like in CollectorEmulator); fix bugs;

- detector.py: currently main working file; Event, EventDetector, and CollectorEmulator classes inside (TBD: remove emulator to a separate file with commandline tools);

- detector_legacy.py: "ancient" project for event detector, currently deprecated, but some code bieces could be useful.

- settings_template.py: settings.py is required file for backend, but it contains sensitive data (SM credentials). This is a template for settings.