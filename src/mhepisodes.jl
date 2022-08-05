using SAPRINCore
using Arrow
using DataFrames
"Map episode start flag to StartType"
function mapstart(enumeration, born, participation, inmigration, locationentry, externalresstart)
    inmigration = inmigration != 1 ? locationentry : inmigration #locationentry treated as inmigration
    # find which flag is set
    z = findfirst(collect((born, enumeration, inmigration, externalresstart, participation)) .== 1)
    return z == nothing ? 6 : z #if no flag is set return 6 = attributechange
end
"Map episode end flags to EndType"
function mapend(died, refusal, ltf, current, outmigration, locationexit, extresend)
    outmigration = outmigration != 1 ? locationexit : outmigration #locationentry treated as inmigration
    z = findfirst(collect((died, outmigration, extresend, refusal, ltf, 0, current)) .== 1)
    return z == nothing ? 6 : z #if no flag is set return 6 = attributechange
end
"""
Convert SAPRIN SurveillanceEpisodesYrAgeDeliveryParents to mental Health Data Prize format
"""
function produce_mhepisodes(node::String)
    nodeid = Int8(findfirst(values(settings.Nodes) .== node))
    df = Arrow.Table(joinpath(episodepath(node), "SurveillanceEpisodesYrAgeDeliveryParents_batched.arrow")) |> DataFrame
    df.NodeId .= nodeid
    df.IsUrbanOrRural .= nodeid == 1 || nodeid == 3 ? Int8(1) : Int8(3)
    df.SpouseId .= missing
    e = select(df, :NodeId, :IndividualId, :DoB, :DoD, :CalendarYear, :Age, :Sex, :LocationId, :HouseholdId, :HHRelationshipTypeId => :HHRelation, :IsUrbanOrRural,
        :MotherId, :FatherId, :SpouseId, :StartDate, :EndDate,
        [:Enumeration, :Born, :Participation, :InMigration, :LocationEntry, :ExtResStart] => ByRow((e, b, p, i, l, x) -> mapstart(e, b, p, i, l, x)) => :StartType,
        [:Died, :Refusal, :LostToFollowUp, :Current, :OutMigration, :LocationExit, :ExtResEnd] => ByRow((d, r, l, c, o, a, x) -> mapend(d, r, l, c, o, a, x)) => :EndType,
        :Episode, :Episodes, :Resident, :MotherStatus, :FatherStatus, :ChildrenEverBorn)
    open(joinpath(episodepath(node), "IndividualExposureEpisodes.arrow"), "w") do io
        Arrow.write(io, e, compress=:zstd)
    end
    return nothing
end
produce_mhepisodes("DIMAMO")