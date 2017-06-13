
# TODO implement buffering


function Data.schema(rp::ResultProxy)
    cols = columns(rp)
    nrows = rowcount(rp)
    Data.Schema(name.(cols), eltype.(cols), nrows)
end


mutable struct Source
    res::ResultProxy
    schema::Data.Schema
    status::Bool
    ncurrentrow::Int

    currentrow::Dict
    nextrow::Dict  # TODO consider whether this is the right way of doing this!

    function Source(res::ResultProxy; load_first_row::Bool=true)
        currentrow = Dict()
        if load_first_row
            arr = fetchmany(res, 1)
            if length(arr) > 0
                status = false
                nextrow = arr[end]
            else
                status = true
                nextrow = Dict()
            end
        else
            status = false
            nextrow = Dict()
        end
        new(res, Data.schema(res), status, 0, currentrow, nextrow)
    end
end

function pullrow!(src::Source, row::Integer)
    if row > src.ncurrentrow
        arr = fetchmany(src.res, row - src.ncurrentrow)
        if length(arr) < (row - src.ncurrentrow)
            src.ncurrentrow += length(arr)
            src.status = true
        else
            src.currentrow = arr[end]
            src.ncurrentrow = row
        end
    elseif row < src.ncurrentrow
        throw(ArgumentError("SQL doesn't support back-tracking to previous rows. I know, WTF?"))
    end
end

Base.getindex(src::Source, col::String) = src.schema[col]

getfield(src::Source, col::Int) = src.currentrow[src.schema.header[col]]
getfield(src::Source, col::String) = src.currentrow[col]

Data.schema(src::Source) = src.schema

# this is currently fucked up, don't know if SQLAlchemy exposes it
Data.isdone(src::Source, row=1, col=1) = src.status

Data.size(src::Source) = size(src.schema)

Data.streamtype(::Type{Source}, ::Type{Data.Field}) = true

# TODO there seems to be no way around doing this ridiculous dict lookup at least once

function Data.streamfrom{T}(src::Source, ::Type{Data.Field}, ::Type{T}, row, col)
    pullrow!(src, row)
    convert(T, getfield(src, col))::T
end

function Data.streamfrom{T}(src::Source, ::Type{Data.Field}, ::Type{Nullable{T}}, row, col)
    pullrow!(src, row)
    val = getfield(src, col)
    val == nothing && (return Nullable{T}())
    Nullable{T}(convert(T, val))
end
