using DataFrames
using Arrow
using Dates
using Statistics
using ShiftedArrays
using Missings
# read Residencies
function newrandomdate(base::Date,startdate::Date,enddate::Date)
    return base + Dates.Day(trunc(Int64, Dates.value(enddate - startdate) * rand(Float64)))
end
function test(basedirectory::String, node::String)
    r = Arrow.Table(joinpath(basedirectory, node, "Staging", "IndividualResidenciesIntermediate.arrow")) |> DataFrame
    println("$(nrow(r)) residence rows")
    #r = r[r.IndividualId .<= 100,:]
    rs = Arrow.Table(joinpath(basedirectory, node, "Staging", "ResidentStatus.arrow")) |> DataFrame
    println("$(nrow(rs)) residence status rows")
    #rs = rs[rs.IndividualId .<= 100,:]
    s = outerjoin(r, rs, on=[:IndividualId => :IndividualId, :LocationId => :LocationId])
    #s = s[s.IndividualId .<= 567,:]
    dropmissing!(s, :LocationId, disallowmissing = true)
    dropmissing!(s, :Gap, disallowmissing = true)
    replace!(s.ResidentStatus, missing => 1)
    disallowmissing!(s, [:IndividualId,:StartDate,:StartType,:EndDate,:EndType, :GapStart, :GapEnd, :ResidentIndex, :ResidentStatus])
    
    for i = 1:nrow(s)
        od = s[i,:ObservationDate]
        if ismissing(od)
            s[i,:ObservationDate]=s[i,:StartDate]
        end
    end
    disallowmissing!(s,[:ObservationDate, :ResidentStatus])
    s = s[((s.ObservationDate .>= s.StartDate) .& (s.ObservationDate .<= s.EndDate)), :] #drop records where observation date is out of bounds
    df = combine(groupby(s, :IndividualId), :LocationId, :StartDate, :StartType, :EndDate, :EndType, :ResidentIndex, :Gap, :GapStart, :GapEnd, :ObservationDate, :ResidentStatus,
                :ResidentStatus => Base.Fix2(lag,1) => :LastResidentStatus, 
                :LocationId => Base.Fix2(lag,1) => :LastLocationId, :Gap => Base.Fix2(lag,1) => :LastGap)
    insertcols!(df,:episode => 0)
    episode = 0
    for i = 1:nrow(df)
        if ismissing(df[i,:LastLocationId])
            episode = 1
            df[i,:episode]=episode
        else
            location = df[i,:LocationId]
            lastlocation = df[i,:LastLocationId]
            gap = df[i,:Gap]
            lastgap = df[i,:LastGap]
            if location != lastlocation || gap != lastgap
                episode = episode+1
                df[i,:episode] = episode
            else
                residentstatus = df[i,:ResidentStatus]
                lastresidentstatus =df[i,:LastResidentStatus]
                if residentstatus != lastresidentstatus
                    episode = episode+1
                    df[i,:episode] = episode
                else
                    df[i,:episode] = episode
                end
            end
        end
    end
    s = combine(groupby(df, [:IndividualId,:LocationId,:episode]), :StartDate => first => :StartDate, :StartType => first => :StartType, 
    :EndDate => first => :EndDate, :EndType => first => :EndType, 
    :Gap => first => :Gap,
    :GapStart => first => :GapStart, :GapEnd => first => :GapEnd,
    :ResidentStatus => first => :ResidentStatus, :ResidentIndex => mean => :ResidentIndex,
    :ObservationDate => minimum => :StartObservationDate, :ObservationDate => maximum => :EndObservationDate)
    println("Node $(node) $(nrow(s)) episodes after resident split")
    df = combine(groupby(s, :IndividualId), :LocationId, :episode, :StartDate, :StartType, :EndDate, :EndType, :Gap, :GapStart, :GapEnd, :ResidentStatus, :StartObservationDate, :EndObservationDate, :ResidentIndex,
                :ResidentStatus => Base.Fix2(lead,1) => :NextResidentStatus, 
                :ResidentStatus => Base.Fix2(lag,1) => :LastResidentStatus, 
                :StartObservationDate => Base.Fix2(lead,1) => :NextStartObsDate,
                :EndObservationDate => Base.Fix2(lag,1) => :LastEndObsDate)
    for i = 1:nrow(df)
        if df[i,:ResidentStatus] == 1 && !ismissing(df[i,:NextResidentStatus]) && df[i,:NextResidentStatus] == 2
            df[i,:EndDate] = newrandomdate(df[i,:EndObservationDate],df[i,:EndObservationDate],df[i,:NextStartObsDate])
            df[i,:EndType] = 4
        end
        if df[i,:ResidentStatus] == 1 && !ismissing(df[i,:LastResidentStatus]) && df[i,:LastResidentStatus] == 2
            df[i,:StartDate] = newrandomdate(df[i,:LastEndObsDate],df[i,:LastEndObsDate],df[i,:StartObservationDate])
            df[i,:StartType] = 3
        end
    end
    filter!(:ResidentStatus => s -> s == 1, df)
    println("Node $(node) $(nrow(df)) episodes after dropping non-resident episodes")
    df.ResidenceId = 1:nrow(df)
    select!(df,[:ResidenceId, :IndividualId, :LocationId, :StartDate,:StartType, :EndDate, :EndType, :StartObservationDate, :EndObservationDate, :ResidentIndex])
    mv(joinpath(basedirectory, node, "Staging", "IndividualResidencies.arrow"),joinpath(basedirectory, node, "Staging", "IndividualResidencies_old.arrow"), force=true)
    Arrow.write(joinpath(basedirectory, node, "Staging", "IndividualResidencies.arrow"), df, compress=:zstd)
    return nothing
end # test
df = test("D:\\Data\\SAPRIN_Data","Agincourt")