Koala
======
Slightly nicer dump of (most) TRUST messages from Network Rail's feed

Dependencies
------
* Python 3.x
* [stomp.py](https://pypi.python.org/pypi/stomp.py)

Config
------
Move config.json.example to config.json

=== `username`, `passsword`
You'll need to insert an email address
and password for a Network Rail open data account. You can sign up [here](https://datafeeds.networkrail.co.uk/ntrod/login).

It can take several days for your account to become active, and you'll have to
specifically add TRUST to your account (log in and add "All TOCs" to the list
under the "Train Movements" tab). 

=== `subscribe`
The subscribed topic can be for all train movements (the default), for specific
passenger TOCs, or for all freight movements. These topics are detailed [here](http://nrodwiki.rockshore.net/index.php/Train_Movements)

=== `identifier`
The identifier needs to be unique for the instance (as it's used to give you a
period of messages while disconnected)

Credits and such
------

I wouldn't have been able to put this together without the NROD wiki, maintained
by Peter Hicks. I'm also grateful for the pointers he offered when I talked to
him about the TRUST feed.
<http://nrodwiki.rockshore.net/>
