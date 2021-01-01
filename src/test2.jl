using DataFrames
using Arrow
using Dates

df = Arrow.Table(joinpath("D:\\Data\\SAPRIN_Data","Agincourt","Staging","IndividualResidencies.arrow")) |> DataFrame