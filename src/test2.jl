using Dates
using Arrow
using DataFrames

s = Arrow.Table(joinpath("D:\\Data\\SAPRIN_Data", "Agincourt","Staging","ResidentStatus.arrow")) |> DataFrame
