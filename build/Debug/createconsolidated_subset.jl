using Dates
using Arrow
using DataFrames
using TableBrowse

preferredhhbatches = Arrow.Stream(joinpath("D:\\Data\\SAPRIN_Data", "DIMAMO", "DayExtraction", "IndividualPreferredHHConsolidatedDays_batched.arrow"))
hstate = iterate(preferredhhbatches)
t = now()
h, hst = hstate
hd = h |> DataFrame
s = filter(row -> row.IndividualId == 399, hd)
Arrow.write(joinpath("D:\\Data\\SAPRIN_Data", "DIMAMO", "DayExtraction", "IndividualPreferredHHConsolidatedDays_subset.arrow"), s, compress=:zstd)
browse(s)
