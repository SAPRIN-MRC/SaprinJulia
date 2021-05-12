using Dates
using Arrow
using DataFrames
using TableBrowse

preferredhhbatches = Arrow.Stream(joinpath("D:\\Data\\SAPRIN_Data", "DIMAMO", "DayExtraction", "DayDatasetStep02_batched.arrow"))
#preferredhhbatches = Arrow.Stream(joinpath("D:\\Data\\SAPRIN_Data", "Agincourt", "DayExtraction", "ResDaysNoMember_batched.arrow"))
hstate = iterate(preferredhhbatches)
t = now()
h, hst = hstate
hd = h |> DataFrame
s = filter(row -> row.IndividualId == 11100, hd)
browse(s)
