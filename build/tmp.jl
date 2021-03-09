using DataFrames
using Arrow
using Dates
using Statistics
using ShiftedArrays
using Missings
using CategoricalArrays
using TableOperations
using Tables
using Statistics
using SAPRINCore
using XLSX
function addsheet!(path, df, sheetname)
    XLSX.openxlsx(path, mode ="rw") do xf
        sheet = XLSX.addsheet!(xf, sheetname)
        data = collect(eachcol(df))
        cnames = DataFrames.names(df)
        XLSX.writetable!(sheet, data, cnames)
    end
end
function basicepisodeQA(basedirectory, node)
    df = Arrow.Table(joinpath(basedirectory, node, "Episodes", "SurveillanceEpisodesBasic_batched.arrow")) |> DataFrame
    sf = combine(groupby(df, [:Born, :Enumeration, :InMigration, :LocationEntry, :ExtResStart, :Participation, :MembershipStart]), nrow => :n)
    sf[!,:Born] = convert.(Int64, sf[:, :Born]) #needed by XLSX
    sf[!,:Enumeration] = convert.(Int64, sf[:, :Enumeration]) #needed by XLSX
    sf[!,:InMigration] = convert.(Int64, sf[:, :InMigration]) #needed by XLSX
    sf[!,:LocationEntry] = convert.(Int64, sf[:, :LocationEntry]) #needed by XLSX
    sf[!,:ExtResStart] = convert.(Int64, sf[:, :ExtResStart]) #needed by XLSX
    sf[!,:Participation] = convert.(Int64, sf[:, :Participation]) #needed by XLSX
    sf[!,:MembershipStart] = convert.(Int64, sf[:, :MembershipStart]) #needed by XLSX
    sort!(sf)
    XLSX.writetable(joinpath(basedirectory, node, "Episodes","QC", "EpisodesQA.xlsx"), collect(eachcol(sf)),names(sf),overwrite=true,sheetname="StartFlags")
    sf = combine(groupby(df, [:Died, :OutMigration, :LocationExit, :ExtResEnd, :Refusal, :LostToFollowUp, :Current, :MembershipEnd]), nrow => :n)
    sf[!,:Died] = convert.(Int64, sf[:, :Died]) #needed by XLSX
    sf[!,:OutMigration] = convert.(Int64, sf[:, :OutMigration]) #needed by XLSX
    sf[!,:LocationExit] = convert.(Int64, sf[:, :LocationExit]) #needed by XLSX
    sf[!,:ExtResEnd] = convert.(Int64, sf[:, :ExtResEnd]) #needed by XLSX
    sf[!,:Refusal] = convert.(Int64, sf[:, :Refusal]) #needed by XLSX
    sf[!,:LostToFollowUp] = convert.(Int64, sf[:, :LostToFollowUp]) #needed by XLSX
    sf[!,:Current] = convert.(Int64, sf[:, :Current]) #needed by XLSX
    sf[!,:MembershipEnd] = convert.(Int64, sf[:, :MembershipEnd]) #needed by XLSX
    sort!(sf)
    addsheet!(joinpath(basedirectory, node, "Episodes","QC", "EpisodesQA.xlsx"), sf, "EndFlags")
    sf = filter(r -> r.Episode < r.Episodes & r.Current == 1, df)
    sf[!,:Resident] = convert.(Int64, sf[:, :Resident]) #needed by XLSX
    sf[!,:Sex] = convert.(Int64, sf[:, :Sex]) #needed by XLSX
    sf[!,:Born] = convert.(Int64, sf[:, :Born]) #needed by XLSX
    sf[!,:Enumeration] = convert.(Int64, sf[:, :Enumeration]) #needed by XLSX
    sf[!,:InMigration] = convert.(Int64, sf[:, :InMigration]) #needed by XLSX
    sf[!,:LocationEntry] = convert.(Int64, sf[:, :LocationEntry]) #needed by XLSX
    sf[!,:ExtResStart] = convert.(Int64, sf[:, :ExtResStart]) #needed by XLSX
    sf[!,:Participation] = convert.(Int64, sf[:, :Participation]) #needed by XLSX
    sf[!,:MembershipStart] = convert.(Int64, sf[:, :MembershipStart]) #needed by XLSX
    sf[!,:Died] = convert.(Int64, sf[:, :Died]) #needed by XLSX
    sf[!,:OutMigration] = convert.(Int64, sf[:, :OutMigration]) #needed by XLSX
    sf[!,:LocationExit] = convert.(Int64, sf[:, :LocationExit]) #needed by XLSX
    sf[!,:ExtResEnd] = convert.(Int64, sf[:, :ExtResEnd]) #needed by XLSX
    sf[!,:Refusal] = convert.(Int64, sf[:, :Refusal]) #needed by XLSX
    sf[!,:LostToFollowUp] = convert.(Int64, sf[:, :LostToFollowUp]) #needed by XLSX
    sf[!,:Current] = convert.(Int64, sf[:, :Current]) #needed by XLSX
    sf[!,:MembershipEnd] = convert.(Int64, sf[:, :MembershipEnd]) #needed by XLSX
    sf[!,:Gap] = convert.(Int64, sf[:, :Gap]) #needed by XLSX
    addsheet!(joinpath(basedirectory, node, "Episodes","QC", "EpisodesQA.xlsx"), sf, "CurrentBeforeEnd")
    sf = filter(r -> r.Episode == 1, df) #Start episodes only
    sf = combine(groupby(sf, [:Born, :Enumeration, :InMigration, :LocationEntry, :ExtResStart, :Participation, :MembershipStart]), nrow => :n)
    sf[!,:Born] = convert.(Int64, sf[:, :Born]) #needed by XLSX
    sf[!,:Enumeration] = convert.(Int64, sf[:, :Enumeration]) #needed by XLSX
    sf[!,:InMigration] = convert.(Int64, sf[:, :InMigration]) #needed by XLSX
    sf[!,:LocationEntry] = convert.(Int64, sf[:, :LocationEntry]) #needed by XLSX
    sf[!,:ExtResStart] = convert.(Int64, sf[:, :ExtResStart]) #needed by XLSX
    sf[!,:Participation] = convert.(Int64, sf[:, :Participation]) #needed by XLSX
    sf[!,:MembershipStart] = convert.(Int64, sf[:, :MembershipStart]) #needed by XLSX
    sort!(sf)
    addsheet!(joinpath(basedirectory, node, "Episodes","QC", "EpisodesQA.xlsx"), sf, "Episode1StartFlags")
    sf = filter(r -> r.Episode == r.Episodes, df) #Last episodes only
    sf = combine(groupby(df, [:Died, :OutMigration, :LocationExit, :ExtResEnd, :Refusal, :LostToFollowUp, :Current, :MembershipEnd]), nrow => :n)
    sf[!,:Died] = convert.(Int64, sf[:, :Died]) #needed by XLSX
    sf[!,:OutMigration] = convert.(Int64, sf[:, :OutMigration]) #needed by XLSX
    sf[!,:LocationExit] = convert.(Int64, sf[:, :LocationExit]) #needed by XLSX
    sf[!,:ExtResEnd] = convert.(Int64, sf[:, :ExtResEnd]) #needed by XLSX
    sf[!,:Refusal] = convert.(Int64, sf[:, :Refusal]) #needed by XLSX
    sf[!,:LostToFollowUp] = convert.(Int64, sf[:, :LostToFollowUp]) #needed by XLSX
    sf[!,:Current] = convert.(Int64, sf[:, :Current]) #needed by XLSX
    sf[!,:MembershipEnd] = convert.(Int64, sf[:, :MembershipEnd]) #needed by XLSX
    sort!(sf)
    addsheet!(joinpath(basedirectory, node, "Episodes","QC", "EpisodesQA.xlsx"), sf, "LastEpisodeEndFlags")
   return nothing
end

@info "Started execution $(now())"
t = now()
# testreadwrite("D:\\Data\\SAPRIN_Data","Agincourt")
# @info "Finished Agincourt $(now())"
basicepisodeQA("D:\\Data\\SAPRIN_Data","DIMAMO")
@info "Finished DIMAMO $(now())"
#testreadwrite("D:\\Data\\SAPRIN_Data","AHRI")
d = now()-t
@info "Stopped execution $(now()) duration $(round(d, Dates.Second))"
