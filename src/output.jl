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

_fetchmulti(rp::ResultProxy, nrows::Integer) = nrows ≥ 0 ? pyfetchmany(rp, nrows) : pyfetchall(rp)

# for now this does fetchall by default if nrows < 0
# this is ridiculously inefficient but I don't think much can be done
# works for DataFrames
function fetchmany(::Type{T}, cols::AbstractVector{Column}, rp::ResultProxy, nrows::Integer) where T
    # cols = columns(rp)
    colnames = name.(cols)
    coltypes = Type[Union{eltype(col),Null} for col ∈ cols]
    arr = convert(Array, _fetchmulti(rp, nrows))
    df = T(coltypes, Symbol.(colnames), length(arr))
    for (row,tpl) ∈ enumerate(arr)
        vals = convert(Tuple, tpl)
        for (col,dtype) ∈ enumerate(coltypes)
            if vals[col] == pynone
                df[row,col] = null
            else
                df[row, col] = convert(dtype, vals[col])
            end
        end
    end
    df
end
function fetchmany(cols::AbstractVector{Column}, rp::ResultProxy, nrows::Integer)
    fetchmany(DataFrame, cols, rp, nrows)
end
fetchmany(::Type{T}, rp::ResultProxy, nrows::Integer) where T = fetchmany(T, columns(rp), rp, nrows)
fetchmany(rp::ResultProxy, nrows::Integer) = fetchmany(DataFrame, rp, nrows)

fetchall(::Type{T}, cols::AbstractVector{Column}, rp::ResultProxy) where T = fetchmany(T, cols, rp, -1)
fetchall(::Type{T}, rp::ResultProxy) where T = fetchall(T, columns(rp), rp)

fetchall(cols::AbstractVector{Column}, rp::ResultProxy) = fetchall(DataFrame, cols, rp)
fetchall(rp::ResultProxy) = fetchall(DataFrame, rp)

