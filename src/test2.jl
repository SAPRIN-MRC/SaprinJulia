using Dates
using Arrow
using DataFrames

df =  Arrow.Table(joinpath("D:\\Data\\SAPRIN_Data", "DIMAMO", "Staging", "IndividualMemberships.arrow")) |> DataFrame