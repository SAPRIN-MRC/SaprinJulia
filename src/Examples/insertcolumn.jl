using Random
using DataFrames
using Arrow
using Tables
using TableBrowse

function same(a, b)
    if ismissing(a) || ismissing(b)
        return 0
    else 
        return a == b ? 1 : 0
    end
end
df = DataFrame(a = [1,2,3,4,5,6], b=[1,missing,3,missing,5,missing], c=[missing,2,missing,4,missing,6])
#insertcols!(df,findfirst(occursin.(names(df),"bb")),:d => Int8(0))
s = transform(df, [:a, :b] => ((x,y) -> same.(x,y)) => :d)
browse(s)