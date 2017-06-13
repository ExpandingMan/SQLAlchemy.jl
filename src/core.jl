abstract type Wrapped end
Base.getindex(w::Wrapped, key) = w.o[key]

unwrap(x) = x
unwrap(x::Wrapped) = x.o
unwrap(x::Union{Tuple,Vector}) = map(unwrap, x)

unwrap_kw(x) = [(ξ[1], unwrap(ξ[2])) for ξ in x]

macro wrap_type(typename)
    quote
        struct $(esc(typename)) <: Wrapped
            o::PyObject

            function $(esc(typename))(args...; kwargs...)
                args = unwrap(args)
                kwargs = unwrap_kw(kwargs)
                new(pycall(sqlalchemy[$(string(typename))], PyObject, args...; kwargs...))
            end

            $(esc(typename))(o::PyObject) = new(o)
        end
    end
end

@wrap_type BinaryExpression
@wrap_type Column
@wrap_type Connection
@wrap_type Delete
@wrap_type Engine
@wrap_type Insert
@wrap_type MetaData
@wrap_type ResultProxy
@wrap_type Select
@wrap_type Table
@wrap_type UnaryExpression
@wrap_type Update
@wrap_type ForeignKey

struct SQLFunc <: Wrapped
    o::PyObject
end

struct DelayedSQLFunc
    name::Symbol
    DelayedSQLFunc(name) = new(Symbol(name))
end

Base.show(io::IO, d::DelayedSQLFunc) = print(io, d.name, "(...)")

function (d::DelayedSQLFunc)(args...; kwargs...)
    args = unwrap(args)
    kwargs = unwrap_kw(kwargs)
    SQLFunc(pycall(sqlalchemy["func"][string(d.name)]["__call__"], PyObject, args...; kwargs...))
end

func(name) = DelayedSQLFunc(name)
func(name, arg) = func(name)(arg)

(c::Connection)(args...; kwargs...) = execute(c, args...; kwargs...)

abstract type SQLType <: Wrapped end
abstract type JuliaType end

macro wrap_sql_type(typenames...)
    expr = Expr(:block)
    for typename ∈ typenames
        sqlname = Symbol(string("SQL", typename))
        q = quote
            struct $(esc(sqlname)) <: SQLType
                o::PyObject
                function $(esc(sqlname))(args...; kwargs...)
                    args = unwrap(args)
                    kwargs = unwrap_kw(kwargs)
                    new(sqlalchemy[$(QuoteNode(typename))](args...; kwargs...))
                end
                $(esc(sqlname))(o::PyObject) = new(o)
            end
        end
        push!(expr.args, q)
    end
    expr
end

@wrap_sql_type String Integer Boolean Date DateTime Enum Float Interval Numeric
@wrap_sql_type Text Time Unicode UnicodeText

const jl_sql_type_map = Dict(Int=>SQLInteger, Bool=>SQLBoolean, Float64=>SQLFloat,
                             String=>SQLString)


for (jl_type, sql_type) ∈ jl_sql_type_map
    unwrap{T<:jl_type}(::Type{T}) = unwrap(sql_type())
end

function Base.convert(::Type{JuliaType}, s::Wrapped)
    if s isa ForeignKey return Int end
    for (k,v) ∈ jl_sql_type_map
        if s isa v return k end
    end
    error("No corresponding Julia type for $s")
end

function Base.convert(::Type{SQLType}, s)
    for (k,v) ∈ jl_sql_type_map
        if s <: k
            return v()
        end
    end
    error("No corresponding SQL type for $s")
end


struct Other <: Wrapped
    o::PyObject
end

struct DelayedFunction
    args
    kwargs
    fname
end

function Base.show(io::IO, d::DelayedFunction)
    print(io, d.fname, "(")
    print(io, "_, ")
    isempty(d.args) || print(io, join(d.args, ", "))
    isempty(d.kwargs) || print(io, ";", join(d.kwargs, ", "))
    print(io, ")")
end

(d::DelayedFunction)(arg::Wrapped) = d.fname(arg, d.args...; d.kwargs...)

macro define_method(typename, method, jlname, ret, generic::Bool=true)
    o = quote
        function $(esc(jlname))(arg::$typename, args...; kwargs...)
            args = unwrap(args)
            kwargs = unwrap_kw(kwargs)
            # TODO this construct is very bad, figure out how to change it
            try
                val = pycall(unwrap(arg)[$(string(method))], PyObject, args...; kwargs...)
                $ret(val)
            catch err
                if err isa KeyError
                    args = [arg, args...]
                    DelayedFunction(args, kwargs, $jlname)
                else
                    rethrow(err)
                end
            end
            # val = pycall(unwrap(arg)[$(string(method))], PyObject, args...; kwargs...)
            # $ret(val)
        end
    end
    if generic
        o = quote
            $o
            $(esc(jlname))(args...; kwargs...) = DelayedFunction(args, kwargs, $jlname)
        end
    end
    o
end

macro define_top(method, jlname, ret)
    quote
        function $(esc(jlname))(args...; kwargs...)
            args = unwrap(args)
            kwargs = unwrap_kw(kwargs)
            val = pycall(sqlalchemy[$(string(method))], PyObject, args...; kwargs...)
            $ret(val)
        end
    end
end

@define_method(MetaData,create_all,createall,Other)
@define_method(Table,insert,insert,Insert)
@define_method(Insert,values,Base.values,Insert)
@define_method(Insert,compile,compile,Insert)
@define_method(Engine,connect,Base.connect,Connection)
@define_method(Connection,execute,execute,ResultProxy)
@define_method(Connection,close,Base.close,identity)
@define_method(Update,where,wear,Update)
@define_method(Select,where,wear,Select,false)
@define_method(Select,select_from,selectfrom,Select)
@define_method(Select,and_,and,Select)
@define_method(Select,order_by,orderby,Select)
@define_method(Select,group_by,groupby,Select)
@define_method(Select,having,having,Select)
@define_method(Select,distinct,distinct,Select)
@define_method(Select,limit,limit,Select)
@define_method(Select,offset,offset,Select)
@define_method(Table,alias,alias,Other)
@define_method(Table,delete,delete,Delete)
@define_method(Table,update,update,Update)
@define_method(Delete,where,wear,Delete,false)

# these are wrapped again in output.jl
@define_method(ResultProxy,fetchone,pyfetchone,PyObject)
@define_method(ResultProxy,fetchall,pyfetchall,PyObject)
@define_method(ResultProxy,fetchmany,pyfetchmany,PyObject)

@define_method(SQLFunc,label,label,SQLFunc)

Base.join(t1::Table, t2::Table; kwargs...) = Select(t1.o[:join](unwrap(t2); kwargs...))

Base.print(io::IO, w::Wrapped) = print(io, unwrap(w)[:__str__]())

Base.show(io::IO, w::Wrapped) = print(io, unwrap(w)[:__repr__]())

@define_top(create_engine,createengine,Engine)
@define_top(select,Base.select,Select)
@define_top(text,text,Select)
@define_top(desc,desc,UnaryExpression)
@define_top(asc,asc,UnaryExpression)

inspect(w::Wrapped) = Other(inspection[:inspect](unwrap(w)))

Base.getindex(t::Table, column_name) = Column(unwrap(t)["c"][string(column_name)])


for (op, py_op) in zip([:(==), :(>), :(>=), :(<), :(<=), :(!=)],
                       [:__eq__, :__gt__, :__ge__, :__lt__, :__le__, :__ne__])
    @eval function $op(c1::Column, c2::Union{Column, AbstractString, Number})
        BinaryExpression(pycall(unwrap(c1)[$(string(py_op))], PyObject, unwrap(c2)))
    end
end
