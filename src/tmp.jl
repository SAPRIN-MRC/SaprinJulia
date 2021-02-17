using DataFrames
using Arrow
using Dates
using Statistics
using ShiftedArrays
using Missings
using CategoricalArrays
using TableOperations
using Tables
using SAPRINCore

function setparentstatus!(r)
    if ismissing(r.MotherDoD)
        r.MotherDead = Int8(-1)
    elseif r.MotherDoD <= r.DayDate
        r.MotherDead = Int8(1)
    end
    if ismissing(r.FatherDoD)
        r.FatherDead = Int8(-1)
    elseif r.FatherDoD <= r.DayDate
        r.FatherDead = Int8(1)
    end
end
"Add individual characteristics to to day records"
function basicepisodes(basedirectory, node)
    individuals = Arrow.Table(joinpath(basedirectory, node, "Staging", "Individuals.arrow")) |> DataFrame
    residentdaybatches = Arrow.Stream(joinpath(basedirectory, node, "DayExtraction", "DayDatasetStep01_batched.arrow"))
    hstate = iterate(residentdaybatches)
    i = 1
    while hstate !== nothing
        t = now()
        @info "Node $(node) batch $(i) at $(t)"
        h, hst = hstate
        hd = h |> DataFrame
        df = innerjoin(hd, individuals, on = :IndividualId)
        insertcols!(df, :MotherDead => Int8(0), :FatherDead => Int8(0))
        for row in eachrow(df)
            setparentstatus!(row)
        end
        select!(df,[:IndividualId, :Sex, :DoB, :DoD, :MotherId, :MotherDead, :FatherId, :FatherDead, :DayDate, :HouseholdId, :LocationId, :Resident, :Gap, 
                   :Enumeration, :Born, :Participation, :InMigration, :LocationEntry, :ExtResStart, 
                   :Died, :Refusal, :LostToFollowUp, :Current, :OutMigration, :LocationExit, :ExtResEnd, :MembershipStart, :MembershipEnd, 
                   :HHRelationshipTypeId, :Memberships, :GapStart, :GapEnd])
        open(joinpath(basedirectory, node, "DayExtraction", "DayDatasetStep02$(i).arrow"),"w"; lock = true) do io
            Arrow.write(io, df, compress=:zstd)
        end
        hstate = iterate(residentdaybatches, hst)
        @info "Node $(node) batch $(i) completed after $(round(now()-t, Dates.Second))"
        i = i + 1
    end
    combinedaybatch(basedirectory,node,"DayDatasetStep02", i-1)
    return nothing
end
@info "Started execution $(now())"
t = now()
# testreadwrite("D:\\Data\\SAPRIN_Data","Agincourt")
# @info "Finished Agincourt $(now())"
addindividualattributes("D:\\Data\\SAPRIN_Data","DIMAMO")
@info "Finished DIMAMO $(now())"
#testreadwrite("D:\\Data\\SAPRIN_Data","AHRI")
d = now()-t
@info "Stopped execution $(now()) duration $(round(d, Dates.Second))"
