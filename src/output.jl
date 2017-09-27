#===================================================================================================
    <metadata>
===================================================================================================#
# TODO use Dates.Time for time-of-day objects
const SQL_TYPE_DICT = Dict("INTEGER"=>Int64, "FLOAT"=>Float64, "VARCHAR"=>String,
                           "BIT"=>Bool, "SMALLDATETIME"=>DateTime, "BIGINT"=>BigInt,
                           "BINARY"=>BitArray, "BLOB"=>BitArray, "BOOLEAN"=>Bool,
                           "CHAR"=>String, "CLOB"=>String, "DATE"=>Date,
                           "DATETIME"=>DateTime, "DECIMAL"=>String, "FLOAT"=>Float64,
                           "INT"=>Int64, "JSON"=>Dict, "NCHAR"=>String,
                           "NVARCHAR"=>String, "NUMERIC"=>Float64, "REAL"=>Float64,
                           "SMALLINT"=>Int16, "TEXT"=>String, "TIME"=>String,
                           "TIMESTAMP"=>DateTime, "VARBINARY"=>BitArray, "STRING"=>String,
                           "VARCHAR"=>String)


function _sql_coltype(col::PyObject)
    sqltype = split(string(col[:type]), ' ')[2]
    # TODO not sure this will always work, use regex
    sqltype = split(sqltype, '(')[1]
    get(SQL_TYPE_DICT, uppercase(sqltype), Any)
end


fetchone{T}(::Type{T}, rp::ResultProxy) = convert(T, pyfetchone(rp))
fetchone(::Type{Dict}, rp::ResultProxy) = convert(PyCall.PyAny, pyfetchone(rp))
fetchone(rp::ResultProxy) = fetchone(Array, rp)

fetchmany{T}(::Type{T}, rp::ResultProxy, size::Int=1)  = convert(T, pyfetchmany(rp, size))
fetchmany(rp::ResultProxy, size::Int=1) = fetchmany(Array, rp, size)

import Base.eltype

Base.eltype(c::Column) = _sql_coltype(c.o)

rowcount(rp::ResultProxy) = rp.o[:rowcount]

eltypes(cols::Vector{Column}) = eltype.(cols)
eltypes(cols::Dict{String,Column}) = Dict{String,DataType}(k=>eltype(v) for (k,v) ∈ cols)
export eltypes

name(c::Column)::String = c.o[:name]

columns(t::Table) = [Column(v) for v ∈ t.o[:columns][:values]()]
columns(rp::ResultProxy) = [Column(v) for v ∈ rp.o[:context][:compiled][:statement][:columns][:values]()]

# returns a dict giving columns as sqlalchemy objects
columns(::Type{Dict}, t::Table) = Dict{String,Column}(k=>Column(v) for (k,v) ∈ t.o[:columns])
function columns(::Type{Dict}, rp::ResultProxy)
    Dict{String,Column}(k=>Column(v) for (k,v) ∈ rp.o[:context][:compiled][:statement][:columns])
end
export columns
#===================================================================================================
    </metadata>
===================================================================================================#

# this is the fallback method for tabular constructors
# works for DataFrames and DataTables
function fetchall{T}(::Type{T}, rp::ResultProxy)
    cols = columns(rp)
    colnames = name.(cols)
    coltypes = eltype.(cols)
    arr = convert(Array, pyfetchall(rp))
    df = T(coltypes, Symbol.(colnames), length(arr))
    for (row,dict) ∈ enumerate(arr)
        for k ∈ keys(dict)
            df[row, Symbol(k)] = dict[k]
        end
    end
    df
end


#===================================================================================================
    <DataFrames>
===================================================================================================#
#===================================================================================================
    </DataFrames>
===================================================================================================#


