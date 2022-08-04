using Dates
using Arrow
using DataFrames
using TableBrowse

df = Arrow.Stream(joinpath("D:\\Data\\SAPRIN_Data", "DIMAMO", "DayExtraction", "DayDatasetStep04_batched.arrow"))
hstate = iterate(df)
h, hst = hstate
hd = h |> DataFrame
println(names(hd))
