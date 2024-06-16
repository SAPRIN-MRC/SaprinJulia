using Arrow
using DataFrames
using Dates
using FreqTables
using Plots
using SAPRINCore
using Tables

function currentpopulation(node::String)
  df = Arrow.Table(joinpath(episodepath(node), "SurveillanceEpisodesYrAge.arrow")) |> DataFrame
  s=nrow(unique(df[(df.Current .==1),:],:IndividualId))
  s=println("Current Population of $node is $s individuals")
  return nothing
end

function residentstatusanalysisbyyear(node::String)
  df = Arrow.Table(joinpath(stagingpath(node), "ResidentStatus.arrow")) |> DataFrame
  s = select(df, :IndividualId, :LocationId, :ObservationDate, :ResidentStatus, :ObservationDate => ByRow(x -> Dates.year(x)) => :ObservationYear)
  a = unstack(combine(groupby(s, [:ObservationYear, :ResidentStatus]), nrow => :count),[:ObservationYear], :ResidentStatus, :count, renamecols = x->Symbol(x == 1 ? "Resident" : "Nonresident"))
  transform!(a, [:Resident, :Nonresident] => ByRow( (x,y) -> y/(x+y)) => :NonResProportion)
  #   years = Dates.year.(df.ObservationDate) 
#   a = freqtable(years, df.ResidentStatus)
  return a
  end

function residentstatusanalysis(node::String)
  df = Arrow.Table(joinpath(episodepath(node), "SurveillanceEpisodesYrAge.arrow")) |> DataFrame
  for i = 1:nrow(df)
   if df[i, :EndDate] > Date(2022,06,30)
      df[i, :Current] = 1
      df[i, :Died] = 0
      df[i, :OutMigration] = 0
      df[i, :LocationExit] = 0
      df[i, :ExtResEnd] = 0
      df[i, :Refusal] = 0
      df[i, :MembershipEnd] = 0
      df[i, :OutMigration] = 0
   end
  end
   df= df[(df.StartDate .<= Date(2022,06,30) .&& df.Current .==1) ,:]
   filter!([:StartDate, :EndDate] => (s, e) -> s <= e, df)
   Pop =nrow(unique(df[(df.Current .==1),:],:IndividualId))
   Pop15 =nrow(df[(df.Current .==1 .&& df.Age .< 15),:])
   Pop15Perc =(Pop15/Pop)*100
   a = unstack(combine(groupby(df, [:Resident]), nrow => :count), :Resident, :count, renamecols = x->Symbol(x == 1 ? "Resident" : "Nonresident"))
   transform!(a, [:Resident, :Nonresident] => ByRow( (x,y) -> (y/(x+y))*100) => :NonResProportion)

 
  println(" ")
  println("Population of $node is on 01 July 2022 is $Pop individuals. Under 15 Population is  $Pop15Perc%")
  println("$a")
  println(" ")

  return nothing
end

function uniqueindividuals(node)
  df = Arrow.Table(joinpath(episodepath(node), "SurveillanceEpisodesYrAge.arrow")) |> DataFrame
  individuals = combine(groupby(df, :IndividualId), nrow => :episodes)
  return nrow(individuals)
end

function uniquelocations(node)
  df = Arrow.Table(joinpath(episodepath(node), "SurveillanceEpisodesYrAge.arrow")) |> DataFrame
  locations= combine(groupby(df, :LocationId), nrow => :episodes)
  return nrow(locations)
end

function uniquehouseholds(node)
  df = Arrow.Table(joinpath(episodepath(node), "SurveillanceEpisodesYrAge.arrow")) |> DataFrame
  households= combine(groupby(df, :HouseholdId), nrow => :episodes)
  return nrow(households)
end

function persondays(node)
  df = Arrow.Table(joinpath(episodepath(node), "SurveillanceEpisodesYrAge.arrow"))
  return sum(df.Days)
end


#currentpopulation("Agincourt")
#currentpopulation("DIMAMO")
#currentpopulation("AHRI")

#residentstatusanalysis("Agincourt")
#residentstatusanalysis("DIMAMO")
#residentstatusanalysis("AHRI")

#agincourtyrs = persondays("Agincourt")/365.25
#dimamoyrs = persondays("DIMAMO")/365.25
#hriyrs = persondays("AHRI")/365.25
#println("Agincourt PersonYears $agincourtyrs")
#println("DIMAMO PersonYears $dimamoyrs")
#println("AHRI PersonYears $ahriyrs")