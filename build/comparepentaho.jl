using DataFrames
using Arrow
using SAPRINCore
using XLSX
using CSV

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
    select!(pe, :IndividualUid, :IndividualId => :PentahoId, :StartDate => :PentahoStart, :EndDate => :PentahoEnd, :Episodes => :PentahoEpisodes)
    je = innerjoin(finaljulia, juliaindividualmap, on = :IndividualId)
    select!(je, :IndividualUid, :IndividualId => :JuliaId, :StartDate => :JuliaStart, :EndDate => :JuliaEnd, :Episodes => :JuliaEpisodes)
    e = outerjoin(pe, je, on = :IndividualUid)
    d = filter(r -> r.PentahoEpisodes != r.JuliaEpisodes, e)
    XLSX.writetable(joinpath(basedirectory, node, "Episodes","QC", "EpisodeComparison.xlsx"), collect(eachcol(d)),names(d),overwrite=true, sheetname="EpisodeNumbers")
    #filter start episodes
    startpentaho = filter(r -> r.Episode == 1, pentahoepisodes)
    startjulia = filter(r -> r.Episode == 1, juliaepisodes)
    pe = innerjoin(startpentaho, pentahoindividualmap, on = :IndividualId)
    select!(pe, :IndividualUid, :IndividualId => :PentahoId, :StartDate => :PentahoStart, :EndDate => :PentahoEnd, :Resident => :P_Resident, :Born => :P_Born, :Enumeration => :P_Enumeration, :InMigration => :P_InMigration, :LocationEntry => :P_LocationEntry, :ExtResStart => :P_ExtResStart)
    je = innerjoin(startjulia, juliaindividualmap, on = :IndividualId)
    select!(je, :IndividualUid, :IndividualId => :JuliaId, :StartDate => :JuliaStart, :EndDate => :JuliaEnd, :Resident => :J_Resident, :Born => :J_Born, :Enumeration => :J_Enumeration, :InMigration => :J_InMigration, :LocationEntry => :J_LocationEntry, :ExtResStart => :J_ExtResStart)
    e = outerjoin(pe, je, on = :IndividualUid)
    d = filter(r -> r.P_Born != r.J_Born, e)
    d[!,:P_Born] = convert.(Int64, d[:, :P_Born]) #needed by XLSX
    d[!,:P_Enumeration] = convert.(Int64, d[:, :P_Enumeration]) #needed by XLSX
    d[!,:P_InMigration] = convert.(Int64, d[:, :P_InMigration]) #needed by XLSX
    d[!,:P_LocationEntry] = convert.(Int64, d[:, :P_LocationEntry]) #needed by XLSX
    d[!,:P_ExtResStart] = convert.(Int64, d[:, :P_ExtResStart]) #needed by XLSX
    d[!,:P_Resident] = convert.(Int64, d[:, :P_Resident]) #needed by XLSX
    d[!,:J_Born] = convert.(Int64, d[:, :J_Born]) #needed by XLSX
    d[!,:J_Enumeration] = convert.(Int64, d[:, :J_Enumeration]) #needed by XLSX
    d[!,:J_InMigration] = convert.(Int64, d[:, :J_InMigration]) #needed by XLSX
    d[!,:J_LocationEntry] = convert.(Int64, d[:, :J_LocationEntry]) #needed by XLSX
    d[!,:J_ExtResStart] = convert.(Int64, d[:, :J_ExtResStart]) #needed by XLSX
    d[!,:J_Resident] = convert.(Int64, d[:, :J_Resident]) #needed by XLSX
    XLSX.openxlsx(joinpath(basedirectory, node, "Episodes","QC", "EpisodeComparison.xlsx"), mode ="rw") do xf
        sheet = XLSX.addsheet!(xf, "Born")
        data = collect(eachcol(d))
        cnames = DataFrames.names(d)
        XLSX.writetable!(sheet, data, cnames)
    end
    return nothing
end

@info "Started execution $(now())"
t = now()
compareepisodes("D:\\Data\\SAPRIN_Data","DIMAMO")
@info "Finished DIMAMO $(now())"
d = now()-t
@info "Stopped execution $(now()) duration $(round(d, Dates.Second))"
