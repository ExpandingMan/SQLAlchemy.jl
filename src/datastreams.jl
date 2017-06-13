
const DEFAULT_BUFFER_SIZE = 2^16

# NOTE: there is no way around the ridiculous dict lookup in streamfrom because the result doesn't
# even expose a column order

# TODO consider using named tuples instead of dicts
# TODO implement a sane buffer, like a dataframe


#===================================================================================================
    <buffer>
===================================================================================================#
mutable struct Buffer{T}
    data::Vector{T}
    offset::Int  # the difference between the first row of buffer and first row of table
end

Buffer(v::Vector{T}, row::Integer) where T = Buffer{T}(v, row)

hasindex(b::Buffer, i::Integer) = (1 ≤ i - b.offset ≤ length(b))

Base.length(b::Buffer) = length(b.data)

Base.getindex(b::Buffer, i) = b.data[i - b.offset]

Base.setindex!{T}(b::Buffer{T}, val::T, i::Integer) = (b.data[i - b.offset] = val)
Base.setindex!{T}(b::Buffer{T}, val::AbstractVector{T}, i::AbstractVector) = (b.data[i - b.offset] = val)

Base.start(b::Buffer) = b.offset + 1
Base.endof(b::Buffer) = b.offset + length(b)

function slide(b::Buffer, v::AbstractVector)
    n = length(v)
    newbuff = Buffer(similar(b.data), b.offset + n)
    newbuff.data .= circshift(b.data, -n)
    newbuff.data[(end-n+1):end] .= v
    newbuff
end

function slidefull(b::Buffer, v::AbstractVector)
    n = length(v)
    newbuff = Buffer(similar(v), b.offset+n)
    newbuff.data .= v
    newbuff
end

function slidefull!(b::Buffer, v::AbstractVector)
    b.data .= v
    b.offset += length(v)
    b
end

extract{T}(b::Buffer, ::Type{T}, row, col)::T = b[row][col]::T

# for now assume contents of buffer are not nullable type
function extract{T}(b::Buffer, ::Type{Nullable{T}}, row, col)::Nullable{T}
    v = b[row][col]
    v == nothing && (return Nullable{T}())
    Nullable{T}(v)
end
#===================================================================================================
    </buffer>
===================================================================================================#

function Data.schema(rp::ResultProxy)
    cols = columns(rp)
    nrows = rowcount(rp)
    Data.Schema(name.(cols), eltype.(cols), nrows)
end

mutable struct Source
    res::ResultProxy
    schema::Data.Schema
    depleted::Bool

    buffer::Buffer{Dict}

    function Source(res::ResultProxy, sch::Data.Schema;
                    buffer_size::Int=DEFAULT_BUFFER_SIZE, preload::Bool=true)
        if preload
            b = Buffer{Dict}(fetchmany(res, buffer_size), 0)
            depleted = length(b) < buffer_size
        else
            b = Buffer{Dict}(Vector{Dict}(), 0)
            depleted = false  # assume not depleted, but don't know
        end
        new(res, sch, depleted, b)
    end

    function Source(res::ResultProxy; buffer_size::Int=DEFAULT_BUFFER_SIZE, preload::Bool=true)
        Source(res, Data.schema(res), buffer_size=buffer_size, preload=preload)
    end
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
        src.buffer = Buffer{Dict}(v, src.buffer.offset + length(src.buffer.offset))
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
        println("pulling, ", row, " ", src.buffer.offset, " ", length(src.buffer))
        return extract(src.buffer, T, row, col)::T
    else
        println(row, " ", src.buffer.offset)
        return extract(src.buffer, T, row, col)::T
    end
end

function Data.streamfrom{T}(src::Source, ::Type{Data.Field}, ::Type{T}, row, col::Integer)
    Data.streamfrom(src, Data.Field, T, row, Data.header(src)[col])
end
