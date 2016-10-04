# CS 5414 Projects

This class is blazed.

## Tests Custom

### Two Nodes
- crashPartialPreCommit1: Testing case where new leader is elected but he’s the only one so he has to move on by examining his current state
- crashPartialPrecommit2: 2 should become new leader and then commit, but making sure that the new coordinator who has already doesn’t commit to the playlist again, meanwhile everyone else does
- crashVoteREQ1: Participant is uncertain so should lead to abort
- voteNO: Regular case testing vote no
- crashAfterAck: Crash after ack test, should be able to unilaterally commit


### Three Nodes
- oneManDown: participant crashed after vote, but rest move on
- oneManDown2: participant crashes after ack, but rest of the guys move on
- reelection: coordinator crashes after votereq but reelection happens
- cascadingCrash: crash -> reelect -> crash -> reelct -> complete transaction
- roundRobin: correct reelect order round robin
- crashPartialCommit2: testing termination gather where new leader commited and other is undecided
- crashPartialCommit3: testing termination gather where new leader undecided and other is commited


### Recovery Tests

### Test 1

Making sure stable storage logging works for basic addition. 

```
1 start 4 10003
2 start 4 10004
-1 add song url 
```

### Test 2

Ensuring logging for successive transactions.

```
1 start 4 10003
2 start 4 10004
3 start 4 10005 
-1 add song url 
-1 add song2 url2
```

### Test 3 

Coordinator crashes after VOTE-REQ, both processes crash after voting.  

```
1 start 4 10003
2 start 4 10004 
3 start 4 10005
1 crashVoteREQ 2 3 
3 crashAfterVote 
2 crashAfterVote
-1 add song url
1 start 4 10006
2 start 4 10007 
3 start 4 10008
``` 

Newly elected coordinator should be 2, abort. 


