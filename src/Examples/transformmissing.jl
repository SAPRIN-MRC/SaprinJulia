using Random
using DataFrames
using Arrow
using Tables
using TableBrowse

df = DataFrame(a = [1,2,3,4,5,6], b=[1,missing,3,missing,5,missing], c=[missing,2,missing,4,missing,6])
transform!(df, AsTable([:b,:c]) => ByRow(x -> ismissing(x.b) ? x.c : x.b) => :d, :b => ByRow(x -> ismissing(x) ? 0 : 1) => :e)
for row in eachrow(df)
    if ismissing(row.b)
        row.d = row.a * 2
    end
end
browse(df)