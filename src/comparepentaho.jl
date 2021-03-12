using DataFrames
using Arrow
using SAPRINCore
using XLSX
using CSV
using Dates
using Logging

function compareepisodes(basedirectory, node)
    #read pentaho episodes
    pentahoepisodes = CSV.File(joinpath(basedirectory, node, "Episodes", "SurveillanceEpisodesBasicAnon.csv")) |> DataFrame
    pentahoindividualmap = CSV.File(joinpath(basedirectory, node, "Episodes", "IndividualMap.csv")) |> DataFrame
    juliaepisodes = Arrow.Table(joinpath(basedirectory, node, "Episodes", "SurveillanceEpisodesBasic_batched.arrow")) |> DataFrame
    juliaindividualmap = Arrow.Table(joinpath(basedirectory, node, "Staging", "IndividualMap.arrow")) |> DataFrame
    #filter final episodes
    finalpentaho = filter(r -> r.Episode == r.Episodes, pentahoepisodes)
    @info "Node $(node) pentaho final episodes $(nrow(finalpentaho)))"
    finaljulia = filter(r -> r.Episode == r.Episodes, juliaepisodes)
    @info "Node $(node) julia final episodes $(nrow(finaljulia)))"
    #add IndividualUid to episodes
    pe = innerjoin(finalpentaho, pentahoindividualmap, on = :IndividualId)
    select!(pe, :IndividualUid, :IndividualId => :PentahoId, :StartDate => :PentahoStart, :EndDate => :PentahoEnd, :Episodes => :PentahoEpisodes, :Died => :P_Died) 
    je = innerjoin(finaljulia, juliaindividualmap, on = :IndividualId)
    select!(je, :IndividualUid, :IndividualId => :JuliaId, :StartDate => :JuliaStart, :EndDate => :JuliaEnd, :Episodes => :JuliaEpisodes, :Died => :J_Died)
    e = innerjoin(pe, je, on = :IndividualUid)
    d = filter(r -> r.PentahoEpisodes != r.JuliaEpisodes, e)
    transform!(d, names(d, Int8) .=> ByRow(Int), renamecols=false) #needed by XLSX
    transform!(d, names(d, Int32) .=> ByRow(Int), renamecols=false) #needed by XLSX
    transform!(d, names(d, Int16) .=> ByRow(Int), renamecols=false) #needed by XLSX
    XLSX.writetable(joinpath(basedirectory, node, "Episodes","QC", "EpisodeComparison.xlsx"), collect(eachcol(d)),names(d),overwrite=true, sheetname="EpisodeNumbers")
    #filter start episodes
    startpentaho = filter(r -> r.Episode == 1, pentahoepisodes)
    startjulia = filter(r -> r.Episode == 1, juliaepisodes)
    pe = innerjoin(startpentaho, pentahoindividualmap, on = :IndividualId)
    select!(pe, :IndividualUid, :IndividualId => :PentahoId, :StartDate => :PentahoStart, :EndDate => :PentahoEnd, :Resident => :P_Resident, :Born => :P_Born, :Enumeration => :P_Enumeration, :InMigration => :P_InMigration, :LocationEntry => :P_LocationEntry, :ExtResStart => :P_ExtResStart)
    je = innerjoin(startjulia, juliaindividualmap, on = :IndividualId)
    select!(je, :IndividualUid, :IndividualId => :JuliaId, :StartDate => :JuliaStart, :EndDate => :JuliaEnd, :Resident => :J_Resident, :Born => :J_Born, :Enumeration => :J_Enumeration, :InMigration => :J_InMigration, :LocationEntry => :J_LocationEntry, :ExtResStart => :J_ExtResStart)
    e = innerjoin(pe, je, on = :IndividualUid)
    d = filter(r -> r.P_Born != r.J_Born, e)
    transform!(d, names(d, Int8) .=> ByRow(Int), renamecols=false) #needed by XLSX
    transform!(d, names(d, Int32) .=> ByRow(Int), renamecols=false) #needed by XLSX
    transform!(d, names(d, Int16) .=> ByRow(Int), renamecols=false) #needed by XLSX
    addsheet!(joinpath(basedirectory, node, "Episodes","QC", "EpisodeComparison.xlsx"), d, "Born")
    #Compare Died flag
    pe = innerjoin(finalpentaho, pentahoindividualmap, on = :IndividualId)
    select!(pe, :IndividualUid, :IndividualId => :PentahoId, :StartDate => :PentahoStart, :EndDate => :PentahoEnd, :Episodes => :PentahoEpisodes, :Died => :P_Died) 
    je = innerjoin(finaljulia, juliaindividualmap, on = :IndividualId)
    select!(je, :IndividualUid, :IndividualId => :JuliaId, :StartDate => :JuliaStart, :EndDate => :JuliaEnd, :Episodes => :JuliaEpisodes, :Died => :J_Died)
    e = innerjoin(pe, je, on = :IndividualUid)
    d = filter(r -> r.J_Died != r.P_Died, e)
    transform!(d, names(d, Int8) .=> ByRow(Int), renamecols=false) #needed by XLSX
    transform!(d, names(d, Int32) .=> ByRow(Int), renamecols=false) #needed by XLSX
    transform!(d, names(d, Int16) .=> ByRow(Int), renamecols=false) #needed by XLSX
    addsheet!(joinpath(basedirectory, node, "Episodes","QC", "EpisodeComparison.xlsx"), d, "Died")
    #Not in Julia
    pe = innerjoin(finalpentaho, pentahoindividualmap, on = :IndividualId)
    select!(pe, :IndividualUid, :IndividualId => :PentahoId, :StartDate => :PentahoStart, :EndDate => :PentahoEnd, :Episodes => :PentahoEpisodes, :Died => :P_Died) 
    je = innerjoin(finaljulia, juliaindividualmap, on = :IndividualId)
    select!(je, :IndividualUid, :IndividualId => :JuliaId, :StartDate => :JuliaStart, :EndDate => :JuliaEnd, :Episodes => :JuliaEpisodes, :Died => :J_Died)
    e = antijoin(pe, je, on = :IndividualUid)
    transform!(e, names(d, Int8) .=> ByRow(Int), renamecols=false) #needed by XLSX
    addsheet!(joinpath(basedirectory, node, "Episodes","QC", "EpisodeComparison.xlsx"), e, "Not in Julia")
    #Not in Pentaho
    pe = innerjoin(finalpentaho, pentahoindividualmap, on = :IndividualId)
    select!(pe, :IndividualUid, :IndividualId => :PentahoId, :StartDate => :PentahoStart, :EndDate => :PentahoEnd, :Episodes => :PentahoEpisodes) 
    je = innerjoin(finaljulia, juliaindividualmap, on = :IndividualId)
    select!(je, :IndividualUid, :IndividualId => :JuliaId, :StartDate => :JuliaStart, :EndDate => :JuliaEnd, :Episodes => :JuliaEpisodes)
    e = antijoin(je, pe, on = :IndividualUid)
    transform!(e, names(d, Int8) .=> ByRow(Int64), renamecols=false) #needed by XLSX
    addsheet!(joinpath(basedirectory, node, "Episodes","QC", "EpisodeComparison.xlsx"), e, "Not in Pentaho")
    return nothing
end

@info "Started execution $(Dates.now())"
t = now()
# compareepisodes("D:\\Data\\SAPRIN_Data","Agincourt")
compareepisodes("D:\\Data\\SAPRIN_Data","DIMAMO")
compareepisodes("D:\\Data\\SAPRIN_Data","AHRI")
@info "Finished compare episodes $(Dates.now())"
d = now()-t
@info "Stopped execution $(Dates.now()) duration $(round(d, Dates.Second))"
