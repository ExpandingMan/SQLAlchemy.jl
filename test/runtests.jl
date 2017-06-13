using Base.Test
using SQLAlchemy
using DataStreams

engine = createengine("sqlite:///:memory:")
metadata = MetaData()

users = Table("Users", metadata,
              Column("name", SQLString()),
              Column("rank", SQLString()),
              Column("age", SQLFloat()),
              Column("date", SQLDate()))

createall(metadata, engine)
db = connect(engine)

db(insert(users), name="Kirk", rank="Capt", age=31)
db(insert(users), name="Spock", rank="Cmdr", age=33)
db(insert(users), name="McCoy", rank=nothing, age=37)

res = db(select([users]))

src = SQLAlchemy.Source(res)

# this is just for testing datastreams
@testset begin
    @test Data.streamfrom(src, Data.Field, String, 1, src["name"]) == "Kirk"
    @test get(Data.streamfrom(src, Data.Field, Nullable{Int}, 1, src["age"]) == Nullable{Int}(31))
    # @test isnull(Data.streamfrom(src, Data.Field, Nullable{String}, 3, src["rank"]))
end


