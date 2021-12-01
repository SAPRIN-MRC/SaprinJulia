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
days = persondays("DIMAMO") + persondays("Agincourt") + persondays("AHRI")
years = days/365.25
individuals = uniqueindividuals("DIMAMO") + uniqueindividuals("Agincourt") + uniqueindividuals("AHRI")
episodes = basicepisodecount("DIMAMO") + basicepisodecount("Agincourt") + basicepisodecount("AHRI")
println("Years $(@sprintf("%.2f", years)) days $(days) individuals $(individuals) episodes $(episodes)")