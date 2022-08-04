using Arrow
using DataFrames
using SAPRINCore
using Printf
using Tables

function persondays(node)
    df = Arrow.Table(joinpath(episodepath(node), "SurveillanceEpisodesBasic_batched.arrow"))
    # sf = combine(df, :Days => sum => :PersonDays)
    return sum(df.Days)
end

function basicepisodecount(node)
    df = Arrow.Table(joinpath(episodepath(node), "SurveillanceEpisodesBasic_batched.arrow")) |> DataFrame
    return nrow(df)
end

function uniqueindividuals(node)
    df = Arrow.Table(joinpath(episodepath(node), "SurveillanceEpisodesBasic_batched.arrow")) |> DataFrame
    individuals = combine(groupby(df, :IndividualId), nrow => :episodes)
    return nrow(individuals)
end
agincourtyrs = persondays("Agincourt")/365.25
dimamoyrs = persondays("DIMAMO")/365.25
ahriyrs = persondays("AHRI")/365.25
years = agincourtyrs + dimamoyrs + ahriyrs
agincourtindividuals = uniqueindividuals("Agincourt")
dimamoindividuals = uniqueindividuals("DIMAMO")
ahriindividuals = uniqueindividuals("AHRI")
individuals =  agincourtindividuals + dimamoindividuals + dimamoindividuals
episodes = basicepisodecount("DIMAMO") + basicepisodecount("Agincourt") + basicepisodecount("AHRI")
println("Person years\t $(@sprintf("%.0f", agincourtyrs))\t $(@sprintf("%.0f", dimamoyrs))\t $(@sprintf("%.0f", ahriyrs))\t $(@sprintf("%.0f", years))")
println("Unique individuals\t $(@sprintf("%.0f", agincourtindividuals))\t $(@sprintf("%.0f", dimamoindividuals))\t $(@sprintf("%.0f", ahriindividuals))\t $(@sprintf("%.0f", individuals))")
println("Years $(@sprintf("%.2f", years)) individuals $(individuals) episodes $(episodes)")
