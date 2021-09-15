using Arrow
using DataFrames
using Dates
using FreqTables
using SAPRINCore
using Plots

function residentstatusanalysis(node::String)
  df = Arrow.Table(joinpath(stagingpath(node), "ResidentStatus.arrow")) |> DataFrame
  s = select(df, :IndividualId, :LocationId, :ObservationDate, :ResidentStatus, :ObservationDate => ByRow(x -> Dates.year(x)) => :ObservationYear)
  a = unstack(combine(groupby(s, [:ObservationYear, :ResidentStatus]), nrow => :count),[:ObservationYear], :ResidentStatus, :count, renamecols = x->Symbol(x == 1 ? "Resident" : "Nonresident"))
  transform!(a, [:Resident, :Nonresident] => ByRow( (x,y) -> y/(x+y)) => :NonResProportion)
  #   years = Dates.year.(df.ObservationDate) 
#   a = freqtable(years, df.ResidentStatus)
  return a
end

function plotresult(data)
    plot(data.ObservationYear, data.NonResProportion, 
    title = "Proportion Non-resident statusobservations by year", 
    label = "Agincourt",
    xlabel = "Year",
    ylabel = "Proportion")
end

a = residentstatusanalysis("Agincourt")
plotresult(a)