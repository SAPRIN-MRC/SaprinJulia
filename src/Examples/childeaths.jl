using Arrow
using DataFrames
using XLSX
using Dates

function sumdeaths()
  df = Arrow.Table(joinpath("D:\\Data\\SAPRIN_Data","IndividualExposureEpisodesAll.arrow")) |> DataFrame
  childdeaths = subset(df, :EndType => ByRow(x -> x == 1), :Age => ByRow(x -> x <= 4), :CalendarYear => ByRow(x -> x >= 2018 && x <= 2021))
  select!(childdeaths, :NodeId, :IndividualId, :DoB, :DoD, :Age, :CalendarYear, :DoD => ByRow(x -> trunc(Int,Dates.dayofyear(x)/7)+1) => :Week)
  gd = combine(groupby(childdeaths,[:CalendarYear,:Week,:Age], sort = true), nrow => :Deaths)
  XLSX.writetable(joinpath("D:\\Data\\SAPRIN_Data","ChildDeaths.xlsx"), gd)
end
d = sumdeaths()