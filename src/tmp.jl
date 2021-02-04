using DataFrames
using Arrow
using Dates
using Statistics
using ShiftedArrays
using Missings
using CategoricalArrays
using TableOperations
using Tables

"Recreate membership days for residency days without a membership"
function recoverresidentdays(basedirectory::String, node::String)
    hhresidencies = Arrow.Table(joinpath(basedirectory, node, "DayExtraction", "HouseholdResidencyDays.arrow")) |> DataFrame
    println("Read $(nrow(hhresidencies)) rows for Household Residency Days")
    resdaysnomembership = Arrow.Table(joinpath(basedirectory, node, "DayExtraction", "ResDaysNoMember_batched.arrow")) |> DataFrame
    println("Read $(nrow(resdaysnomembership)) rows for resdays no membership")
    df = innerjoin(resdaysnomembership,hhresidencies; on = [:DayDate =>:DayDate , :IndResLocationId => :LocationId], makeunique = true)
    insertcols!(df, :HHRelationshipTypeId => Int32(12), :HHRank =>  Int32(12))
    rename!(df, Dict(:StartType => "HHMemStartType", :EndType => "HHMemEndType", :Start => "HHMemStart", :End => "HHMemEnd"))
    select!(df,)
    return df
end
"Consolidate daily exposure for an individual to a single preferred household based on relationship to household head"
function consolidatepreferredhousehold(basedirectory::String, node::String)
    rd = recoverresidentdays(basedirectory, node)
end

@info "Started execution $(now())"
t = now()
# consolidatepreferredhousehold("D:\\Data\\SAPRIN_Data","Agincourt")
# @info "Finished Agincourt $(now())"
df = consolidatepreferredhousehold("D:\\Data\\SAPRIN_Data","DIMAMO")
# @info "Finished DIMAMO $(now())"
# consolidatepreferredhousehold("D:\\Data\\SAPRIN_Data","AHRI")
d = now()-t
@info "Stopped execution $(now()) duration $(round(d, Dates.Second))"
