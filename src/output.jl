#===================================================================================================
    <metadata>
===================================================================================================#
const SQL_TYPE_DICT = Dict("INTEGER"=>Int64, "FLOAT"=>Float64, "VARCHAR"=>String,
                           "BIT"=>Bool, "SMALLDATETIME"=>DateTime, "BIGINT"=>BigInt,
                           "BINARY"=>BitArray, "BLOB"=>BitArray, "BOOLEAN"=>Bool,
                           "CHAR"=>String, "CLOB"=>String, "DATE"=>Date,
                           "DATETIME"=>DateTime, "DECIMAL"=>String, "FLOAT"=>Float64,
                           "INT"=>Int64, "JSON"=>Dict, "NCHAR"=>String,
                           "NVARCHAR"=>String, "NUMERIC"=>Float64, "REAL"=>Float64,
                           "SMALLINT"=>Int16, "TEXT"=>String, "TIME"=>String,
                           "TIMESTAMP"=>DateTime, "VARBINARY"=>BitArray,
                           "VARCHAR"=>String)


function _sql_coltype(col::PyObject)
    sqltype = split(string(col[:type]), ' ')[2]
    # TODO not sure this will always work, use regex
    sqltype = split(sqltype, '(')[1]
    get(SQL_TYPE_DICT, uppercase(sqltype), Any)
end


fetchone{T}(::Type{T}, rp::ResultProxy) = convert(T, pyfetchone(rp))

import Base.eltype

Base.eltype(c::Column) = _sql_coltype(c.o)

eltypes(cols::Dict{String,Column}) = Dict{String,DataType}(k=>eltype(v) for (k,v) ∈ cols)
export eltypes

# returns a dict giving columns as sqlalchemy objects
columns(t::Table) = Dict{String,Column}(k=>Column(v) for (k,v) ∈ t.o[:columns])
function columns(rp::ResultProxy)
    Dict{String,Column}(k=>Column(v) for (k,v) ∈ rp.o[:context][:compiled][:statement][:columns])
end
export columns
#===================================================================================================
    </metadata>
===================================================================================================#

# this is the fallback method for tabular constructors
# works for DataFrames and DataTables
function fetchall{T}(::Type{T}, rp::ResultProxy)
    coltypes = eltypes(columns(rp))
    colnames = Symbol[Symbol(cname) for cname ∈ keys(coltypes)]
    coltypes = DataType[coltypes[string(col)] for col ∈ colnames]
    arr = convert(Array, pyfetchall(rp))
    df = T(coltypes, colnames, length(arr))
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


