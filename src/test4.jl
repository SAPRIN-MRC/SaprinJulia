using Dates
using Arrow
using Dates
using DataFrames

df = Arrow.Table(joinpath("D:\\Data\\SAPRIN_Data", "DIMAMO", "Staging", "HHeadRelationships.arrow")) |> DataFrame