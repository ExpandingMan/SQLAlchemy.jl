using DataFrames
using SQLAlchemy

db, schema = loadchinook()

albums = Table("Album", schema, autoload=true)
artists = Table("Artist", schema, autoload=true)


q = db(select([artists[:Name],
               func("count", albums[:Title]) |> label("# of albums")]) |>
       selectfrom(join(artists, albums)) |>
       groupby(albums[:ArtistId]) |>
       orderby(desc("# of albums")))

df = fetchall(DataFrame, q)

# q = select([artists[:Name], func("count", albums[:Title]) |> label("# of albums")])




