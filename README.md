Ranking Algorithms
======

Take files consisting of user,item,rating and convert them into recommendations of some form.

We rely on Maven for project management.  In order to run the program you need to do the following.

```bash
    mvn clean install
    java -cp target/ranking-0.0.1-SNAPSHOT.jar \
       com.wesleykerr.ranking.CollaborativeFilter \
       -i input -o output
```

The program expects the file to have a single line per user with all of the ratings on that line. Sample file -

```javascript
{"userId":"1","ratings":[{"item":1,"rating":0.7516},{"item":2,"rating":0.284762}]}
{"userId":"2","ratings":[{"item":3,"rating":0.56},{"item":4,"rating":0.726}]}

```
