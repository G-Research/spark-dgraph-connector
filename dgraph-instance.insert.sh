curl -H "Content-Type: application/rdf" "localhost:8080/mutate?commitNow=true" -XPOST -d $'
{
  set {
   _:luke <name> "Luke Skywalker" .
   _:luke <dgraph.type> "Person" .
   _:leia <name> "Princess Leia" .
   _:leia <dgraph.type> "Person" .
   _:han <name> "Han Solo" .
   _:han <dgraph.type> "Person" .
   _:lucas <name> "George Lucas" .
   _:lucas <dgraph.type> "Person" .
   _:irvin <name> "Irvin Kernshner" .
   _:irvin <dgraph.type> "Person" .
   _:richard <name> "Richard Marquand" .
   _:richard <dgraph.type> "Person" .

   _:sw1 <name> "Star Wars: Episode IV - A New Hope" .
   _:sw1 <release_date> "1977-05-25" .
   _:sw1 <revenue> "775000000" .
   _:sw1 <running_time> "121" .
   _:sw1 <starring> _:luke .
   _:sw1 <starring> _:leia .
   _:sw1 <starring> _:han .
   _:sw1 <director> _:lucas .
   _:sw1 <dgraph.type> "Film" .

   _:sw2 <name> "Star Wars: Episode V - The Empire Strikes Back" .
   _:sw2 <release_date> "1980-05-21" .
   _:sw2 <revenue> "534000000" .
   _:sw2 <running_time> "124" .
   _:sw2 <starring> _:luke .
   _:sw2 <starring> _:leia .
   _:sw2 <starring> _:han .
   _:sw2 <director> _:irvin .
   _:sw2 <dgraph.type> "Film" .

   _:sw3 <name> "Star Wars: Episode VI - Return of the Jedi" .
   _:sw3 <release_date> "1983-05-25" .
   _:sw3 <revenue> "572000000" .
   _:sw3 <running_time> "131" .
   _:sw3 <starring> _:luke .
   _:sw3 <starring> _:leia .
   _:sw3 <starring> _:han .
   _:sw3 <director> _:richard .
   _:sw3 <dgraph.type> "Film" .

   _:st1 <name> "Star Trek: The Motion Picture" .
   _:st1 <release_date> "1979-12-07" .
   _:st1 <revenue> "139000000" .
   _:st1 <running_time> "132" .
   _:st1 <dgraph.type> "Film" .
  }
}
' | python -m json.tool | tee dgraph-instance.inserted.json
