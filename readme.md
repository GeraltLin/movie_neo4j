执行代码向csv导入neo4j
```
./neo4j-admin import --mode=csv --database=movie.db --nodes /home/lin/pycode/movie_neo4j/utils/film.csv --nodes /home/lin/pycode/movie_neo4j/utils/movie_type.csv --nodes /home/lin/pycode/movie_neo4j/utils/director.csv --nodes /home/lin/pycode/movie_neo4j/utils/actor.csv --relationships /home/lin/pycode/movie_neo4j/utils/relation_film_type.csv --relationships /home/lin/pycode/movie_neo4j/utils/relation_director_film.csv --relationships /home/lin/pycode/movie_neo4j/utils/relation_director_actor.csv  --relationships /home/lin/pycode/movie_neo4j/utils/relation_actor_film.csv 
```
查询实例
```
MATCH m=(:电影{film:"西虹市首富"})-[*..1]-() RETURN m
```