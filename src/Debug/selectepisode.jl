using Dates
using Arrow
using DataFrames
using TableBrowse

df = Arrow.Table(joinpath("D:\\Data\\SAPRIN_Data", "Agincourt", "Episodes", "SurveillanceEpisodesBasic_batched.arrow")) |> DataFrame
#s = filter(row -> row.OutMigration == 1 && row.ExtResEnd == 1, df)
s = filter(row -> row.IndividualId == 14091 || row.IndividualId == 27360 || row.IndividualId == 29364, df)
browse(s)