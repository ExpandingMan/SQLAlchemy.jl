module SQLAlchemy

using PyCall
using Compat
using Requires

const sqlalchemy = pyimport("sqlalchemy")
const inspection = pyimport("sqlalchemy.inspection")

export Table, Column, MetaData, Engine, Session
export createengine, select, text, connect, func, inspect, query
export createall, insert, values, compile, connect, execute, fetchone, fetchall, wear
export selectfrom, and, orderby, alias, join, groupby
export having, delete, update, distinct, limit, offset, label, loadchinook, desc, asc, dirty
export SQLString, SQLInteger, SQLBoolean, SQLDate, SQLDateTime, SQLEnum, SQLFloat, SQLInterval
export SQLNumeric, SQLText, SQLTime, SQLUnicode, SQLUnicodeText, SQLType
export @table

import Base: getindex, setindex!, convert, show, join, push!, endof, length, in
import Base: all, first, filter

@require DataFrames begin
    import DataFrames: groupby, eltypes
end

@require DataTables begin
    import DataTables: groupby, eltypes
end

include("core.jl")
include("output.jl")
include("chinook.jl")
# include("orm.jl")

end # module SQLAlchemy
