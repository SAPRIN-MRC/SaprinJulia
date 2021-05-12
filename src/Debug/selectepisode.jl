using Dates
using Arrow
using DataFrames
using TableBrowse

df = Arrow.Table(joinpath("D:\\Data\\SAPRIN_Data", "DIMAMO", "Episodes", "SurveillanceEpisodesYrAge_batched.arrow")) |> DataFrame
#s = filter(row -> row.Episode == row.Episodes && row.MembershipStart == 0 && row.Died == 0 && row.OutMigration == 0 && row.ExtResEnd == 0 && row.Refusal == 0 && row.LostToFollowUp == 0 && row.Current == 0, df)
s = filter(row -> row.IndividualId == 11100 || row.IndividualId == 17829, df)
browse(s)