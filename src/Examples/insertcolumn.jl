using Random
using DataFrames
using Arrow
using Tables
using TableBrowse

df = DataFrame(aa = [1,2,3,4,5,6], bb=[1,missing,3,missing,5,missing], cc=[missing,2,missing,4,missing,6])
insertcols!(df,findfirst(occursin.(names(df),"bb")),:d => Int8(0))
browse(df)