using Dates
using Arrow
using Dates
using CSV
memberships = Arrow.Table(joinpath("D:\\Data\\SAPRIN_Data", "Agincourt", "Staging", "IndividualMemberships.arrow")) |> DataFrame
# "D:\\Data\\SAPRIN_Data"
# m = filter([:IndividualId] => id -> id >= 1 && id <= 10, memberships)
