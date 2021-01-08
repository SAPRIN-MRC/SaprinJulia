using Dates
using Arrow
using Dates
using CSV
individualbounds = Arrow.Table(joinpath("D:\\Data\\SAPRIN_Data", "Agincourt","Staging","AssetStatus.arrow")) |> DataFrame
# "D:\\Data\\SAPRIN_Data"
# m = filter([:IndividualId] => id -> id >= 1 && id <= 10, memberships)
