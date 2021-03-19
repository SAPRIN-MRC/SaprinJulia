using DataFrames
using Arrow
using Statistics
using SAPRINCore

function recodebasicepisodes(node::String)
    nodalcodes = Dict([("Agincourt",1),("DIMAMO",2),("AHRI",3)])
    # Read basic Episodes
    df = Arrow.Table(joinpath(episodepath(node),"SurveillanceEpisodesBasic.arrow")) |> DataFrame
    df.IndividualId = df.IndividualId .+ (nodalcodes[node] * 1000000)
    insertcols!(df,:IndividualId, :NodeId => Int8(nodalcodes[node]))
    return df
end

s = vcat(recodebasicepisodes("Agincourt"), recodebasicepisodes("DIMAMO"), recodebasicepisodes("AHRI"))
persondays = combine(groupby(s,[:NodeId]), :Days => sum => :PersonDays, :IndividualId => length∘unique => :Persons)