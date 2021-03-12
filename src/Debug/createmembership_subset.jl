using Dates
using Arrow
using DataFrames
using TableBrowse
using SAPRINCore

readhouseholdmemberships("Agincourt")
hd = Arrow.Table(joinpath("D:\\Data\\SAPRIN_Data", "Agincourt", "Staging", "HouseholdMemberships.arrow")) |> DataFrame
s = filter(row -> row.IndividualId == 151285, hd)
# Arrow.write(joinpath("D:\\Data\\SAPRIN_Data", "Agincourt", "Staging", "HouseholdMemberships_subset.arrow"), s, compress=:zstd)
browse(s)
