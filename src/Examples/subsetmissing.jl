using Random
using DataFrames
using Arrow
using Tables
using TableBrowse
using Random
using BenchmarkTools

function isgreater(x,y)
    if ismissing(y)
        return true
    end
    return x>y
end
#df = DataFrame(a = [1,2,3,4,6,6], b=[1,missing,3,missing,5,missing], c=[missing,2,missing,4,missing,6])
df = DataFrame(a = 1:1000000, b = vec(rand(1:1000, 1000000, 1)))
# turn some to missing
m = vec(rand(1:1000, 100, 1))
allowmissing!(df, :b)
for i in m
    df[i, :b] = missing
end
@btime filter([:a, :b] => (x, y) -> ismissing(y) || x > y, df)
@btime subset(df, [:a, :b] => (x,y) -> isgreater.(x, y))
@btime subset(df, [:a, :b] => ByRow((x, y) -> ismissing(y) || x > y))
#browse(df)