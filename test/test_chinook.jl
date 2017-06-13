# using DataFrames
using SQLAlchemy
import SQLAlchemy.groupby
using DataStreams
using Feather

db, schema = loadchinook()

albums = Table("Album", schema, autoload=true)
artists = Table("Artist", schema, autoload=true)


q = db(select([artists[:Name],
               func("count", albums[:Title]) |> label("NAlbums")]) |>
       selectfrom(join(artists, albums)) |>
       groupby(albums[:ArtistId]) |>
       orderby(desc("NAlbums")))

# row1 = fetchone(Dict, q)
# row2 = fetchone(Array, q)
# df = fetchall(DataFrame, q)

src = SQLAlchemy.Source(q, buffer_size=8)

# sf1(row) = Data.streamfrom(src, Data.Field, String, row, "Name")
# sf2(row) = Data.streamfrom(src, Data.Field, Nullable{Int}, row, "NAlbums")

info("starting stream...")

sink = Data.stream!(src, DataFrame)

info("done.")

# info("writing feather...")
# Data.stream!(src, Feather.Sink, false, Dict{Int,Function}(), "test1.feather")
# info("done.")




