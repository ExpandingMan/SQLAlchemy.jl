__precompile__(true)

module SQLAlchemy

using DataStreams
using DataFrames
using Nulls  # needed for use with datastreams
using PyCall
# using Requires

const sqlalchemy = PyNULL()
const inspection = PyNULL()

const pynone = PyNULL()

# @require DataFrames begin
#     import DataFrames: groupby, eltypes
# end

export Table, Column, MetaData, Engine, Session
export createengine, select, text, connect, func, inspect, query
export createall, insert, values, compile, connect, execute, fetchone, fetchall, fetchmany, wear
export selectfrom, and, orderby, alias, join, groupby
export having, delete, update, distinct, limit, offset, label, loadchinook, desc, asc, dirty
export SQLString, SQLInteger, SQLBoolean, SQLDate, SQLDateTime, SQLEnum, SQLFloat, SQLInterval
export SQLNumeric, SQLText, SQLTime, SQLUnicode, SQLUnicodeText, SQLType
export @table

import Base: getindex, setindex!, convert, show, join, push!, endof, length, in, length, start
import Base: all, first, filter, eltype
import Base: (==), (>), (>=), (<), (<=), (!=)

import DataFrames.eltypes

function __init__()
    copy!(sqlalchemy, pyimport("sqlalchemy"))
    copy!(inspection, pyimport("sqlalchemy.inspection"))
    copy!(pynone, pybuiltin("None"))
end

include("core.jl")
include("output.jl")
include("chinook.jl")
include("datastreams.jl")
# include("orm.jl")

end # module SQLAlchemy
