using DataFrames
using Arrow
using Dates
using Statistics
using ShiftedArrays
using Missings
using XLSX
using CategoricalArrays


function readindividuals(db::String, node::String, basedirectory::String)
    con = ODBC.Connection(db)
    sql = """SELECT
        UPPER(CONVERT(nvarchar(50),I.IndividualUid)) IndividualUid,
        I.Sex,
        CONVERT(date,SE.EventDate) DoB,
        CASE
        WHEN EE.EventTypeId=7 THEN CONVERT(date,EE.EventDate)
        ELSE NULL
        END DoD,
        UPPER(CONVERT(nvarchar(50),I.MotherUid)) MotherUid,
        UPPER(CONVERT(nvarchar(50),I.FatherUid)) FatherUid,
        I.MotherDoD,
        I.FatherDoD
    FROM dbo.Individuals I
        JOIN dbo.Events SE ON I.BirthEventUid=SE.EventUid
        JOIN dbo.Events EE ON I.EndEventUid=EE.EventUid
    """    
    individuals = DBInterface.execute(con, sql;iterate_rows=true) |> DataFrame
    @info "Read $(nrow(individuals)) $(node) individuals"
    sex = freqtable(individuals, :Sex)
    @info "Sex breakdown $(node)" sex
    sort!(individuals, :IndividualUid)
    sql = """SELECT
        UPPER(CONVERT(nvarchar(50),WomanUid)) WomanUid
        , UPPER(CONVERT(nvarchar(50),I.IndividualUid)) ChildUid
        FROM dbo.Pregnancies P
            JOIN dbo.Individuals I ON P.OutcomeEventUid=I.BirthEventUid
    """  
    pregnancies = DBInterface.execute(con, sql;iterate_rows=true) |> DataFrame
    DBInterface.close!(con)
    @info "Read $(nrow(pregnancies)) $(node) pregnancies"
    pregnancies = unique!(pregnancies, :ChildUid)
    @info "Read $(nrow(pregnancies)) $(node) unique children"
    # Add MotherUid from pregnancies
    individuals = leftjoin(individuals, pregnancies, on=:IndividualUid => :ChildUid, makeunique=true, matchmissing=:equal)
    for i = 1:nrow(individuals)
        if ismissing(individuals[i,:MotherUid]) && !ismissing(individuals[i,:WomanUid])
            individuals[i,:MotherUid] = individuals[i,:WomanUid]
        end
    end
    individuals.IndividualId = 1:nrow(individuals)
    # Convert gui ids to integer ids
    map = individuals[!,[:IndividualUid,:IndividualId]]
    # Arrow.write(joinpath(basedirectory, node, "Staging", "IndividualMap.arrow"), map, compress=:zstd)
    # Convert mother and father uids to corresponding integer ids
    individuals = leftjoin(individuals, map, on=:MotherUid => :IndividualUid, makeunique=true, matchmissing=:equal)
    individuals = leftjoin(individuals, map, on=:FatherUid => :IndividualUid, makeunique=true, matchmissing=:equal)
    # Select and rename final columns
    select!(individuals, [:IndividualId,:Sex,:DoB,:DoD,:IndividualId_1,:IndividualId_2,:MotherDoD,:FatherDoD])
    rename!(individuals, :IndividualId_1 => :MotherId, :IndividualId_2 => :FatherId)
    # Fix parent DoDs
    # Mother DoD
    mothers = select(individuals,[:MotherId])
    dropmissing!(mothers)
    unique!(mothers)
    mothers = innerjoin(mothers,individuals, on = :MotherId => :IndividualId, makeunique=true, matchmissing=:equal)
    select!(mothers,[:MotherId, :DoD])
    rename!(mothers,:DoD => :MotherDoD)
    # Father DoD
    fathers = select(individuals,[:FatherId])
    dropmissing!(fathers)
    unique!(fathers)
    fathers = innerjoin(fathers,individuals, on = :FatherId => :IndividualId, makeunique=true, matchmissing=:equal)
    select!(fathers,[:FatherId, :DoD])
    rename!(fathers,:DoD => :FatherDoD)
    individuals = leftjoin(individuals, mothers, on=:MotherId => :MotherId, makeunique=true, matchmissing=:equal)
    individuals = leftjoin(individuals, fathers, on=:FatherId => :FatherId, makeunique=true, matchmissing=:equal)
    for i = 1:nrow(individuals)
        if !ismissing(individuals[i,:MotherDoD_1])
            individuals[i,:MotherDoD] = individuals[i,:MotherDoD_1]
        end
        if !ismissing(individuals[i,:FatherDoD_1])
            individuals[i,:FatherDoD] = individuals[i,:FatherDoD_1]
        end
    end
    select!(individuals,[:IndividualId, :Sex, :DoB, :DoD, :MotherId, :MotherDoD, :FatherId, :FatherDoD])
    disallowmissing!(individuals, [:IndividualId, :Sex, :DoB])
    #Arrow.write(joinpath(basedirectory, node, "Staging", "Individuals.arrow"), individuals, compress=:zstd)
    return individuals
end # readindividuals
node = "AHRI"
readindividuals(settings.Databases[node],node,settings.BaseDirectory)
