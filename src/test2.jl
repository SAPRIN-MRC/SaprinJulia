using Dates
using Arrow
using Dates
using CSV
memberships = Arrow.Table(joinpath("D:\\Data\\SAPRIN_Data", "DIMAMO", "Staging", "IndividualMemberships.arrow")) |> DataFrame
# "D:\\Data\\SAPRIN_Data"