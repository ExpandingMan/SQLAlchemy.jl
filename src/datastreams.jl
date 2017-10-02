# TODO this is largely unfinshed work based on a previous idea that I abandoned because of metadata issues.


function Data.schema(rp::ResultProxy)
    cols = columns(rp)
    nrows = rowcount(rp)
    nrows < 0 && (nrows = null)
    Data.Schema(eltype.(cols), name.(cols), nrows)
end

mutable struct Source
    res::ResultProxy
    schema::Data.Schema
    depleted::Bool

    data::DataFrame

    # function Source(res::ResultProxy, sch::Data.Schema; preload::Bool=true)
    #     if preload
    #         b = Buffer{Dict}(fetchmany(res, buffer_size), 0)
    #         depleted = length(b) < buffer_size
    #     else
    #         b = Buffer{Dict}(Vector{Dict}(), 0)
    #         depleted = false  # assume not depleted, but don't know
    #     end
    #     new(res, sch, depleted, b)
    # end

    # function Source(res::ResultProxy; preload::Bool=true)
    #     Source(res, Data.schema(res), preload=preload)
    # end
end

Data.schema(src::Source) = src.schema
Data.schema(src::Source, ::Type{Data.Field}) = src.schema

Data.header(src::Source) = Data.header(src.schema)

fetchmany(src::Source, size::Int=1) = fetchmany(src.res, size)

Data.isdone(src::Source, row=1, col=1) = src.depleted

Data.streamtype(src::Source, ::Type{Data.Field}) = true
Data.streamtype(::Type{Source}, ::Type{Data.Field}) = true

# doesn't check if depleted
function pullrows!(src::Source, n::Integer)
    v = fetchmany(src, n)
    if length(v) < n && (src.depleted = true)
        src.buffer = Buffer{Dict}(v, src.buffer.offset + length(src.buffer))
        return src.buffer
    end
    slidefull!(src.buffer, fetchmany(src, n))
end

# doesn't check if depleted
function Data.streamfrom{T}(src::Source, ::Type{Data.Field}, ::Type{T}, row, col::String)::T
    if row < start(src.buffer)
        throw(ArgumentError(string("Row $row before start of current buffer $(start(src.buffer)).  ",
                                   "SQL doesn't support backtracking. I know, WTF?")))
    elseif row > endof(src.buffer)  # avoid using this, intended for only one pull
        n = length(src.buffer)
        topull = (row - start(src.buffer)) ÷ n
        for i ∈ 1:topull
            pullrows!(src, n)
        end
        return extract(src.buffer, T, row, col)::T
    else
        return extract(src.buffer, T, row, col)::T
    end
end

function Data.streamfrom{T}(src::Source, ::Type{Data.Field}, ::Type{T}, row, col::Integer)
    Data.streamfrom(src, Data.Field, T, row, Data.header(src)[col])
end
