using DataFrames
using Arrow
using Dates
using Statistics
using ShiftedArrays
using Missings

function individualmemberships(basedirectory::String, node::String)
    memberships = Arrow.Table(joinpath(basedirectory, node, "Staging", "HouseholdMemberships.arrow")) |> DataFrame
    m = similar(memberships,0)
    @time for row in eachrow(memberships)
        tf = DataFrame(row)
        ttf=repeat(tf, Dates.value.(row.EndDate-row.StartDate) + 1)
        ttf.DayDate = ttf.StartDate .+ Dates.Day.(0:nrow(ttf)-1)
        ttf.Start = ttf.DayDate .== ttf.StartDate
        ttf.End = ttf.DayDate .== ttf.EndDate
        append!(m, ttf, cols = :union)
    end
    unique!(m,[:IndividualId,:HouseholdId,:DayDate])
    relationships = Arrow.Table(joinpath(basedirectory, node, "Staging", "HHeadRelationships.arrow")) |> DataFrame
    r = similar(relationships,0)
    @time for row in eachrow(relationships)
        tf = DataFrame(row)
        ttf=repeat(tf, Dates.value.(row.EndDate-row.StartDate) + 1)
        ttf.DayDate = ttf.StartDate .+ Dates.Day.(0:nrow(ttf)-1)
        ttf.Start = ttf.DayDate .== ttf.StartDate
        ttf.End = ttf.DayDate .== ttf.EndDate
        append!(r, ttf, cols = :union)
    end
    unique!(r,[:IndividualId,:HouseholdId,:DayDate])
    @time mr = leftjoin(m, r , on = [:IndividualId => :IndividualId, :HouseholdId => :HouseholdId, :DayDate => :DayDate], makeunique=true, matchmissing=:equal)
    select!(mr,[:MembershipId, :IndividualId, :HouseholdId, :HHRelationshipTypeId, :DayDate, :StartType, :EndType, :StartObservationDate, :EndObservationDate, :Episode])
    replace!(mr.HHRelationshipTypeId, missing => 12)
    disallowmissing!(mr,[:HHRelationshipTypeId, :DayDate])
    show(names(mr))
    show(eltype.(eachcol(mr)))
    println("$(nrow(mr)) day rows")
    mr = combine(groupby(mr,[:IndividualId,:HouseholdId]), :HHRelationshipTypeId, :DayDate, :StartType, :EndType, :StartObservationDate, :EndObservationDate, :Episode, 
                             :HHRelationshipTypeId => Base.Fix2(lag,1) => :LastRelation, :HHRelationshipTypeId => Base.Fix2(lead,1) => :NextRelation)
    @time for i = 1:nrow(mr)
        if !ismissing(mr[i,:LastRelation])
            if mr[i, :LastRelation] != mr[i, :HHRelationshipTypeId]
                mr[i, :StartType] = 104
            end
        end
        if !ismissing(mr[i,:NextRelation])
            if mr[i, :NextRelation] != mr[i, :HHRelationshipTypeId]
                mr[i, :EndType] = 104
            end
        end
    end
    @time m = combine(groupby(mr,[:IndividualId, :HouseholdId, :HHRelationshipTypeId, :Episode]), :DayDate => minimum => :StartDate, :StartType => first => :StartType,
                                                                                            :DayDate => maximum => :EndDate, :EndType => last => :EndType,
                                                                                            :StartObservationDate => first =>:StartObservationDate, :EndObservationDate => first => :EndObservationDate)
    Arrow.write(joinpath(basedirectory, node, "Staging", "IndividualMemberships.arrow"), m, compress=:zstd)
    println("Wrote $(nrow(m)) $(node) individual membership episodes")
    return nothing
end
householdmembershipdays("D:\\Data\\SAPRIN_Data", "DIMAMO")