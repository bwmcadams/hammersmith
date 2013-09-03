
A Note On My Thoughts Regarding "Pinning Connections"
-----------------------------------------------------
Having spent much of the 2 years prior to Dec. 2012 working at 10gen (the company behind MongoDB), I've got far
more intimate a knowledge of the inner workings of MongoDB, its wire protocol and how drivers "should" behave than any
sane human being should. As a result, I've witnessed interesting experiments & mistakes, as well as being party to many
design discussions & arguments.

There is one pervasive "concept" that started in the [official MongoDB Java Driver](http://github.com/mongodb/mongo-java-driver) and has been pushed in the official "driver behavior" documentation
with which I strongly disagree – it is important that you as a user understand where I diverge on the "official driver behavior" if you plan to adopt Hammersmith. I try whereever possible to conform to the official standards for MongoDB driver behaviors and interact properly – *except* in cases where I strongly disagree with a behavior that may harm users. 

My previous Scala driver for MongoDB – [Casbah](http://github.com/mongodb/casbah), the official MongoDB Scala Driver - wrapped the Java driver for the network protocol. I still stand by the decision I made at the time, which was not to reinvent the wheel when there was an existing JVM implementation of the MongoDB network layer – Casbah provided instead wrappers to give support for Scala types & concepts to the Java driver.  But as Casbah grew, and the Scala community grew, the needs of these communities began to diverge from those of the Java community.  It was because of weird concepts from the Java driver, including thread safety issues and blocking behavior (a tremendous problem as the Scala community moves to more asynchronous and non-blocking frameworks) that I embarked upon Hammersmith - to provide a better network layer for Scala + MongoDB, and play nice in the Scala community.

Besides general blocking behavior and questionable thread safety (often "fixed" by the non-performant golden hammer of `synchronized`), there is one major concept with which I fervently disagree and will not implement in Hammersmith - "Pinning Connections". As I said, this concept started in the Java driver, but during the mid-2012 efforts to standardize driver behaviors began being pushed as a "standard" concept that all MongoDB Drivers should use. 

### So what the hell is a "Pinned Connection"?

The concept only applies in languages/environments where user code may talk to MongoDB from multiple threads, and refers to the the fact that once a thread has used a particular connection from the connection pool once, it should
attempt *whenever possible* to use that connection for all future requests in that thread. That, I fervently believe, goes against the idea
of a connection pool. The implementations of this that I've seen try to pretend they aren't completely circumventing the
connection pooling by releasing connections back to the pool, but "favoring" their "last connection" when requesting a new
connection next time the thread is invoked. The current MongoDB Java Driver (at least for r2.10.1 which is "current" at the time of this writing),
implements this behavior, as evinced in the [Pooling code](https://github.com/mongodb/mongo-java-driver/blob/r2.10.1/src/main/com/mongodb/DBPortPool.java#L192-L201).

So what is that code doing? Every time your code needs to communicate with MongoDB, it will go to the `DBPortPool` and attempt to retrieve a connection
for the communication. As shown in this code, it does so by inefficiently rummaging through the pool of available connections and checking
if any of them have a saved `_lastThreadID` attribute which matches their own Thread ID. It is essentially a very weak
(and demonstrably non-thread safe in a number of real world cases) locking mechanism that at the end of the day circumvents many of the
benefits of using a Pool in the first place.

Why, you might ask, would this behavior be baked into a database driver? Essentially, to solve a database consistency issue.
The explanation given to me for this "Connection Pinning" is based on the fact that by default MongoDB until recently considered
a write to be "completed" when it got into the network buffer on the client. If one were not using a connection pool at all, and
immediately did a read looking at the data they just wrote, one might see three possible behaviors (I'm oversimplifying):

    1. The last write succeeded, was applied in the server and the read is "consistent" in that you see the changes you expected.

    2. The last write failed, and unless it was a network error on the client wasn't reported in the client code (that until-recently-default-behavior biting us again)

    3. The write has cleared the client network buffer, but is still in the 'to execute' queue of the connection thread (maybe awaiting a write lock) on the server. In this case we would expect our read operation to block until execution queue of the server's connection clears & commits, eventually getting results similar to #1.

The concerns that lead to "Connection Pinning" relate to possibility #3 – if we *are* using a connection pool, and the last write we did is still queued but we immediately read from a *different connection* than our write was done on. In this case, the read won't block against the pending write – it will read the current view of the data not accounting for the pending write. Potentially providing (at least from the perception of a user) consistency issues. So, the idea of "Connection Pinning" – to try and use the same connection our thread last used, to prevent this issue – was born. Let's still pretend we have the benefits of Connection Pools but circumvent them as much as possible attempting to claim a (very, very) weak lock on our "last" connection.

Now of course, I mention that this "Writes 'succeed' when they hit the client network buffer" was *until recently* the default in MongoDB. As of MongoDB 2.2+, drivers default to waiting for a write to be committed on the server (though only in memory, not on disk) – which while slower than "put it in the network buffer and move on" is saner and less likely to provide user confusion around consistency. Unfortunately, the version of the Java Driver (which I linked above) that includes this change to "wait for writes to commit" *still uses connection pinning*. Worse, the last discussions and documentation I saw around MongoDB Driver Behavior – which referred to 2.2+ behavior – continued to push connection pinning as an expected behavior. I have a creeping fear that we'll see this concept linger for many years to come in the official driver(s).

I am a tremendous fan of MongoDB (as long as you understand what you're getting, such as the consistency behaviors), and my goal in Hammersmith is to write a proper love letter to both Scala + MongoDB. I am taking everything I've learned in the 3 years I spent on Casbah and trying to offer something that does it all **right** – but in some cases my opinions of right differ from those of my now former employer. For a variety of reasons I find "Connection Pinning" to be bad practice. It causes thread safety concerns, slow pool behavior and is just generally ["Crazytown Bananapants"](http://www.youtube.com/watch?v=8Q3AiDVJs2A). Regardless of the way that other MongoDB drivers behave, I *will not* use "Connection Pinning" in Hammersmith.

\* I am, however, a fairly reasonable person. If someone can convince me that I've misunderstood or overblown this issue, or another reason why it makes sense, I may reverse my position.
