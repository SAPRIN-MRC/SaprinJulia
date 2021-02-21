using Dates
using Arrow
using DataFrames
using TableBrowse

df = Arrow.Table(joinpath("D:\\Data\\SAPRIN_Data", "DIMAMO", "Staging", "IndividualResidencies.arrow")) |> DataFrame
s = copy(df)
filter!([:StartType] => (id -> id == 3), s)
browse(s)