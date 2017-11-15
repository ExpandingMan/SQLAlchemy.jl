
#===================================================================================================
    <coercions>

    code for forcing conversions
===================================================================================================#
# this date format is needed because it's more common than Dates.ISODateTimeFormat
const DEFAULT_DATETIME_FORMAT = Dates.DateFormat("yyyy-mm-dd HH:MM:SS.s")

coerce(::Type{T}, x) where T = convert(T, x)::T

coerce(::Type{Union{T,Null}}, x::Void) where T = null

coerce(::Type{Date}, d::AbstractString) = Date(d, Dates.ISODateFormat)
coerce(::Type{DateTime}, d::AbstractString) = DateTime(d, DEFAULT_DATETIME_FORMAT)

function coerce(::Type{Union{D,Null}}, d::AbstractString) where D <: Dates.TimeType
    isempty(d) ? null : coerce(D, d)
end

coerce(::Type{T}, x::PyObject) where T = coerce(T, convert(PyAny, x))::T
#===================================================================================================
    </coercions>
===================================================================================================#

#===================================================================================================
    <metadata>
===================================================================================================#
# TODO use Dates.Time for time-of-day objects
# NOTE: usually "BIGINTEGER" means double ints, NOT arbitrary size ints
const SQL_TYPE_DICT = Dict("INTEGER"=>Int64, "FLOAT"=>Float64, "VARCHAR"=>String,
                           "BIT"=>Bool, "SMALLDATETIME"=>DateTime, "BIGINT"=>BigInt,
                           "BINARY"=>BitArray, "BLOB"=>BitArray, "BOOLEAN"=>Bool,
                           "CHAR"=>String, "CLOB"=>String, "DATE"=>Date,
                           "DATETIME"=>DateTime, "DECIMAL"=>String, "FLOAT"=>Float64,
                           "INT"=>Int64, "JSON"=>Dict, "NCHAR"=>String,
                           "NVARCHAR"=>String, "NUMERIC"=>Float64, "REAL"=>Float64,
                           "SMALLINT"=>Int16, "TEXT"=>String, "TIME"=>String,
                           "TIMESTAMP"=>DateTime, "VARBINARY"=>BitArray, "STRING"=>String,
                           "VARCHAR"=>String, "BIGINTEGER"=>Int64, "NULLTYPE"=>Any)

function _sql_type_string(col::PyObject)
    sqltype = split(string(col[:type]), ' ')[2]
    # TODO not sure this will always work, use regex
    split(sqltype, '(')[1]
end


function _sql_coltype(col::PyObject)
    sqltype = _sql_type_string(col)
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

nulleltypes(cols::AbstractVector{Column}) = Type[Union{eltype(c),Null} for c ∈ cols]

name(c::Column)::String = c.o[:name]

_get_py_columns(t::Table) = t.o[:columns][:values]()
_get_py_columns(rp::ResultProxy) = rp.o[:context][:compiled][:statement][:columns][:values]()

columns(t::Table) = [Column(v) for v ∈ _get_py_columns(t)]
columns(rp::ResultProxy) = [Column(v) for v ∈ _get_py_columns(rp)]

function columns(t::Union{Table,ResultProxy}, cols::AbstractArray{<:AbstractString})
    o = Vector{Column}()
    for c ∈ _get_py_columns(t)
        c.o[:name] ∈ cols && push!(o, Column(c))
    end
    o
end

# returns a dict giving columns as sqlalchemy objects
columns(::Type{Dict}, t::Table) = Dict{String,Column}(k=>Column(v) for (k,v) ∈ t.o[:columns])
function columns(::Type{Dict}, rp::ResultProxy)
    Dict{String,Column}(k=>Column(v) for (k,v) ∈ rp.o[:context][:compiled][:statement][:columns])
end
export columns


"""
    columndict(cols)
    columndict(table)

Returns a dictionary in which the keys are the names of columns and the values are the `Column` objects.
Can pass either column objects or a table.
"""
columndict(cols::AbstractVector{Column}) = Dict(name(col)=>col for col ∈ cols)
columndict(t::Table) = namedict(columns(t))
export namedict


"""
    typedict(cols)
    typedict(table)

Returns a dictionary in which the keys are the names of the columns and the values are the column
data types.  Can pass either column objects or a table.
"""
typedict(cols::AbstractVector{Column}) = Dict(name(col)=>eltype(col) for col ∈ cols)
typedict(t::Table) = typedict(columns(t))
export typedict
#===================================================================================================
    </metadata>
===================================================================================================#

_fetchmulti(rp::ResultProxy, nrows::Integer) = nrows ≥ 0 ? pyfetchmany(rp, nrows) : pyfetchall(rp)

function _insert_single_entry(col::AbstractVector{Union{T,Null}}, row, ncol, val::PyObject) where T
    if val == pynone
        col[row] = null
    else
        col[row] = coerce(Union{T,Null}, val)
    end
end
function _insert_single_entry(col::AbstractVector{T}, row, ncol, val::PyObject) where T
    if val == pynone
        ArgumentError("Tried to insert null into non-null column $ncol.")
    else
        col[row] = coerce(T, val)
    end
end
function _insert_single_entry(col::AbstractVector{T}, row, ncol, val) where T
    col[row] = coerce(T, val)
end

function _insert_row_tuple(cols::AbstractVector, row, vals::Tuple)
    for i ∈ 1:length(cols)
        _insert_single_entry(cols[i], row, i, vals[i])
    end
end

# for now this does fetchall by default if nrows < 0
# this is ridiculously inefficient but I don't think much can be done
# works for DataFrames
function fetchmany(::Type{DataFrame}, colnames::AbstractVector{<:Union{<:AbstractString,Symbol}},
                   coltypes::AbstractVector{<:Type}, rp::ResultProxy, nrows::Integer)
    arr = convert(Array, _fetchmulti(rp, nrows))
    df = DataFrame(coltypes, Symbol.(colnames), length(arr))
    for (row,tpl) ∈ enumerate(arr)
        _insert_row_tuple(df.columns, row, convert(Tuple, tpl))
    end
    df
end
function fetchmany(colnames::AbstractVector{<:Union{<:AbstractString,Symbol}},
                   coltypes::AbstractVector{<:Type}, rp::ResultProxy, nrows::Integer)
    fetchmany(DataFrame, colnames, coltypes, rp, nrows)
end

function fetchmany(::Type{T}, cols::AbstractVector{Column}, rp::ResultProxy, nrows::Integer) where T
    fetchmany(T, name.(cols), nulleltypes(cols), rp, nrows)
end
function fetchmany(cols::AbstractVector{Column}, rp::ResultProxy, nrows::Integer)
    fetchmany(DataFrame, cols, rp, nrows)
end
fetchmany(::Type{T}, rp::ResultProxy, nrows::Integer) where T = fetchmany(T, columns(rp), rp, nrows)
fetchmany(rp::ResultProxy, nrows::Integer) = fetchmany(DataFrame, rp, nrows)

function fetchall(::Type{T}, colnames::AbstractVector{<:Union{<:AbstractString,Symbol}},
                  coltypes::AbstractVector{<:Type}, rp::ResultProxy) where T
    fetchmany(T, colnames, coltypes, rp, -1)
end
function fetchall(colnames::AbstractVector{<:Union{<:AbstractString,Symbol}},
                  coltypes::AbstractVector{<:Type}, rp::ResultProxy)
    fetchmany(colnames, coltypes, rp, -1)
end

fetchall(::Type{T}, cols::AbstractVector{Column}, rp::ResultProxy) where T = fetchmany(T, cols, rp, -1)
fetchall(::Type{T}, rp::ResultProxy) where T = fetchall(T, columns(rp), rp)

fetchall(cols::AbstractVector{Column}, rp::ResultProxy) = fetchall(DataFrame, cols, rp)
fetchall(rp::ResultProxy) = fetchall(DataFrame, rp)
