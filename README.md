# CS 5414 Projects

This class is blazed.



# Recovery Tests 

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


