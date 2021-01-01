using Dates
using Arrow
using Dates
using CSV
#=
rs = Arrow.Table(joinpath("D:\\Data\\SAPRIN_Data","Agincourt", "Staging", "ResidentStatus.arrow")) |> DataFrame
rs = rs[rs.IndividualId .<= 567,:]
=#
rs = Arrow.Table(joinpath("D:\\Data\\SAPRIN_Data","DIMAMO", "Staging", "IndividualMap.arrow")) |> DataFrame
CSV.write(joinpath("D:\\Data\\SAPRIN_Data","DIMAMO", "Staging", "IndividualMap.csv"), rs)