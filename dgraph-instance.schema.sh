curl "localhost:8080/alter" -XPOST -d $'
  director: [uid] .
  name: string @index(term) .
  title: string @lang @index(term) .
  release_date: datetime @index(year) .
  revenue: float .
  running_time: int .
  starring: [uid] .

  type Person {
    name
  }

  type Film {
    title
    release_date
    revenue
    running_time
    starring
    director
  }
' | python -m json.tool

