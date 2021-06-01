using Dates
using Arrow
using DataFrames
using TableBrowse

df = Arrow.Table(joinpath("D:\\Data\\SAPRIN_Data", "DIMAMO", "Staging", "Individuals.arrow")) |> DataFrame
#s = innerjoin(df,df, on = :MotherId => :IndividualId, makeunique = true, matchmissing = :equal)
#
#s = subset(df, :MotherId => x -> x .== 8509, skipmissing = true)
s = subset(df, :MotherDoD => x -> x .!== missing, skipmissing = true)
sort!(s, :MotherId)
browse(s)