using Base.Test
using SQLAlchemy

engine = createengine("sqlite:///:memory:")
metadata = MetaData()

users = Table("Users", metadata,
              Column("rank", SQLString()),
              Column("age", SQLFloat()))

createall(metadata, engine)
db = connect(engine)

db(insert(users), name="Kirk", rank="Capt", age=31)
db(insert(users), name="Spock", rank="Cmdr", age=33)

res1 = db(select([users]))

