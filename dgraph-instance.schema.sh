curl "localhost:8080/alter" -XPOST -d $'
  name: string @index(term) .
  release_date: datetime @index(year) .
  revenue: float .
  running_time: int .

  type Person {
    name
  }

  type Film {
    name
    release_date
    revenue
    running_time
    starring
    director
  }
' | python -m json.tool

